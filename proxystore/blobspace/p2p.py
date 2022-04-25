"""Utilities for establishing peer-to-peer connections."""
from __future__ import annotations

import asyncio
import json
import logging
from types import TracebackType
from typing import Any
from typing import Generator

import websockets
from aiortc import RTCDataChannel
from aiortc import RTCIceCandidate
from aiortc import RTCPeerConnection
from aiortc import RTCSessionDescription
from aiortc.contrib.signaling import BYE
from aiortc.contrib.signaling import object_from_string
from aiortc.contrib.signaling import object_to_string
from websockets import WebSocketServerProtocol

from proxystore.blobspace.messages import P2PConnectionBaseMessage
from proxystore.blobspace.messages import P2PConnectionError
from proxystore.blobspace.messages import P2PConnectionMessage
from proxystore.blobspace.messages import P2PDataTransfer
from proxystore.serialize import deserialize
from proxystore.serialize import SerializationError
from proxystore.serialize import serialize

logger = logging.getLogger(__name__)


class P2PConnection:
    """Peer-to-peer connection.

    Interface for establishing a peer-to-peer connection via WebRTC
    (`aiortc <https://aiortc.readthedocs.io/en/latest/>`_) and
    sending/receiving messages between the two peers. The peer-to-peer
    connection is established using a central and publicly accessible
    signaling server.
    """

    def __init__(self, uuid: str, websocket: WebSocketServerProtocol) -> None:
        """Init P2PConnection.

        Args:
            uuid (str): uuid of this endpoint.
            websocket (WebSocketServerProtocol): websocket connection to the
                signaling server.
        """
        self._uuid = uuid
        self._websocket = websocket
        self._handshake_complete = asyncio.Event()
        self._pc = RTCPeerConnection()
        self._message_queue: asyncio.Queue[bytes] = asyncio.Queue()

        # TODO(gpauloski): create RTCDataChannel that will be used
        # for communication

    @property
    def state(self) -> str:
        """Get the current connection state.

        Returns:
            'connected', 'connecting', 'closed', 'failed', or 'new'.
        """
        return self._pc.connectionState

    async def close(self) -> None:
        """Terminate the peer connection."""
        await self._pc.close()

    async def send(self, message: P2PDataTransfer) -> None:
        """Send message to peer.

        Args:
            message (P2PDataTransfer): data transfer message to send to peer.
        """
        await self._handshake_complete.wait()
        self._channel.send(serialize(message))
        # https://github.com/aiortc/aiortc/issues/547
        await self._channel._RTCDataChannel__transport._data_channel_flush()
        await self._channel._RTCDataChannel__transport._transmit()
        logger.info(f'sent message from {self._uuid}')

    async def recv(self) -> P2PDataTransfer:
        """Receive next message from peer.

        Returns:
            :any:`P2PDataTransfer <proxystore.blobspace.messages.P2PDataTransfer>`  # noqa: E501
            object received from the peer.
        """
        return deserialize(await self._message_queue.get())

    async def send_offer(self, target_uuid: str) -> None:
        """Send offer for peering via signaling server.

        Args:
            target_uuid (str): uuid of target client to establish connection
                with.
        """
        self._channel = self._pc.createDataChannel('p2p')

        @self._channel.on('open')
        def on_open() -> None:
            logger.info(f'opened datachannel with remote {target_uuid}')
            self._handshake_complete.set()

        @self._channel.on('message')
        def on_message(message: bytes) -> None:
            logger.info(f'{self._uuid} got message')
            self._message_queue.put_nowait(message)

        await self._pc.setLocalDescription(await self._pc.createOffer())
        message = serialize(
            P2PConnectionMessage(
                source_uuid=self._uuid,
                target_uuid=target_uuid,
                message=json.dumps(
                    {
                        'offer': object_to_string(self._pc.localDescription),
                    },
                ),
            ),
        )
        await self._websocket.send(message)

    async def send_answer(self, source_uuid: str) -> None:
        """Send answer to peering request via signaling server.

        Args:
            source_uuid (str): uuid of client that sent the initial offer.
        """

        @self._pc.on('datachannel')
        def on_datachannel(channel: RTCDataChannel) -> None:
            logger.info(f'datachannel created by remote {source_uuid}')
            self._channel = channel
            self._handshake_complete.set()

            @channel.on('message')
            def on_message(message: bytes) -> None:
                logger.info(f'{self._uuid} got message')
                self._message_queue.put_nowait(message)

        await self._pc.setLocalDescription(await self._pc.createAnswer())
        message = serialize(
            P2PConnectionMessage(
                source_uuid=self._uuid,
                target_uuid=source_uuid,
                message=json.dumps(
                    {
                        'offer': object_to_string(self._pc.localDescription),
                    },
                ),
            ),
        )
        await self._websocket.send(message)

    async def handle_message(self, message: P2PConnectionBaseMessage) -> None:
        """Handle message from the signaling server.

        Args:
            message (P2PConnectionBaseMessage): message received from the
                signaling server.
        """
        if isinstance(message, P2PConnectionError):
            raise RuntimeError(f'failed to connect to peer: {message.error}')

        assert isinstance(message, P2PConnectionMessage)

        message_data = json.loads(message.message)
        if 'offer' in message_data:
            obj = object_from_string(message_data['offer'])
        elif 'answer' in message_data:  # pragma: no cover
            obj = object_from_string(message_data['answer'])
        else:
            raise AssertionError(
                'P2P connection message does not contain either an offer or '
                'an answer',
            )

        if isinstance(obj, RTCSessionDescription):
            await self._pc.setRemoteDescription(obj)
            if obj.type == 'offer':
                await self.send_answer(message.source_uuid)
        elif isinstance(obj, RTCIceCandidate):  # pragma: no cover
            # We should not receive an RTCIceCandidate message via the
            # signaling server but this is here following the aiortc example.
            # https://github.com/aiortc/aiortc/blob/713fb644b95328f8ec1ac2cbb54def0424cc6645/examples/datachannel-cli/cli.py#L30  # noqa: E501
            await self._pc.addIceCandidate(obj)
        elif obj is BYE:  # pragma: no cover
            raise AssertionError('received BYE message')
        else:
            raise AssertionError('received unknown message')

    async def wait(self) -> None:
        """Wait on P2P connection to be established."""
        await self._handshake_complete.wait()


class P2PConnectionManager:
    """P2P Connection Manager.

    Manages individual connections to multiple peers.

    Note:
        The :class:`P2PConnectionManager <.P2PConnectionManager>` can be
        used as a context manager.

        >>> async with P2PConnectionManager(..) as manager:
        >>>     ...
    """

    def __init__(
        self,
        uuid: str,
        name: str,
        websocket: WebSocketServerProtocol,
    ) -> None:
        """Init P2PConnectionManager.

        Args:
            uuid (str): uuid of the client
            name (str): name of the client. Used for readability in logs.
            websocket (WebSocketServerProtocol): websocket connection to the
                signaling server.
        """
        self._uuid = uuid
        self._name = name
        self._websocket = websocket
        self._peers: dict[frozenset[str], P2PConnection] = {}

        self._handle_task = asyncio.create_task(self._handle_messages())
        logger.info(
            f'P2PConnectionManager(uuid={uuid}, name={name}) initialized',
        )

    async def __aenter__(self) -> P2PConnectionManager:
        """Enter async context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Leave async context manager and close manager."""
        await self.close()

    def __await__(self) -> Generator[Any, None, P2PConnectionManager]:
        """Awaitable constructor."""
        return self.__aenter__().__await__()

    async def _handle_messages(self) -> None:
        """Handle messages from the signaling server.

        Forwards the message to the correct P2PConnection instance.
        """
        while True:
            try:
                message = deserialize(await self._websocket.recv())
            except websockets.exceptions.ConnectionClosedOK:
                break
            except websockets.exceptions.ConnectionClosedError:
                break
            except SerializationError:
                logger.error(
                    'caught deserialization error on message received from '
                    'signaling server... skipping message',
                )
                continue

            logger.info(
                f'P2PConnectionManager(uuid={self._uuid}, name={self._name}) '
                f'received message type {type(message)} from signaling server',
            )

            if isinstance(message, P2PConnectionBaseMessage):
                peers = frozenset({message.source_uuid, message.target_uuid})
                if peers not in self._peers:
                    self._peers[peers] = P2PConnection(
                        self._uuid,
                        self._websocket,
                    )
                await self._peers[peers].handle_message(message)
            else:
                logger.error(
                    'received unknown message type from signaling server: '
                    f'{message}',
                )

    async def close(self) -> None:
        """Close the connection manager.

        Warning:
            Does not close the websocket to the signaling server.
        """
        for connection in self._peers.values():
            await connection.close()
        self._handle_task.cancel()
        logger.info(
            f'P2PConnectionManager(uuid={self._uuid}, name={self._name}) '
            'closed',
        )

    async def get_connection(self, target_uuid: str) -> P2PConnection:
        """Get connection between this client and the target.

        Args:
            target_uuid (str): uuid of client to get connection with.

        Returns:
            :any:`P2PConnection <P2PConnection>`
        """
        peers = frozenset({self._uuid, target_uuid})
        if peers in self._peers:
            return self._peers[peers]
        else:
            return await self.new_connection(target_uuid)

    async def new_connection(self, target_uuid: str) -> P2PConnection:
        """Establish new P2P connection with a client.

        Args:
            target_uuid (str): uuid of client to connect to.

        Returns:
            New :any:`P2PConnection <P2PConnection>` if the connection did not
            already exist otherwise returns the existing connection.
        """
        peers = frozenset({self._uuid, target_uuid})
        if peers in self._peers:
            return self._peers[peers]
        connection = P2PConnection(self._uuid, self._websocket)
        await connection.send_offer(target_uuid)
        self._peers[peers] = connection
        return connection
