"""Representation of peer-to-peer connection."""
from __future__ import annotations

import asyncio
import json
import logging

from aiortc import RTCDataChannel
from aiortc import RTCIceCandidate
from aiortc import RTCPeerConnection
from aiortc import RTCSessionDescription
from aiortc.contrib.signaling import BYE
from aiortc.contrib.signaling import object_from_string
from aiortc.contrib.signaling import object_to_string
from websockets import WebSocketServerProtocol

from proxystore.p2p.messages import PeerConnectionMessage
from proxystore.serialize import serialize

logger = logging.getLogger(__name__)


class PeerConnection:
    """Peer-to-peer connection.

    Interface for establishing a peer-to-peer connection via WebRTC
    (`aiortc <https://aiortc.readthedocs.io/en/latest/>`_) and
    sending/receiving messages between the two peers. The peer-to-peer
    connection is established using a central and publicly accessible
    signaling server.
    """

    def __init__(
        self,
        uuid: str,
        name: str,
        websocket: WebSocketServerProtocol,
    ) -> None:
        """Init P2PConnection.

        Args:
            uuid (str): uuid of this client.
            name (str): readable name of this client for logging.
            websocket (WebSocketServerProtocol): websocket connection to the
                signaling server.
        """
        self._uuid = uuid
        self._name = name
        self._websocket = websocket

        self._handshake_complete = asyncio.Event()
        self._pc = RTCPeerConnection()
        self._message_queue: asyncio.Queue[bytes] = asyncio.Queue()

        self._peer_uuid: str | None = None
        self._peer_name: str | None = None

    @property
    def _log_prefix(self) -> str:
        local = log_name(self._uuid, self._name)
        remote = (
            'pending'
            if self._peer_uuid is None or self._peer_name is None
            else log_name(self._peer_uuid, self._peer_name)
        )
        return f'{self.__class__.__name__}[{local} > {remote}]'

    @property
    def state(self) -> str:
        """Get the current connection state.

        Returns:
            'connected', 'connecting', 'closed', 'failed', or 'new'.
        """
        return self._pc.connectionState

    async def close(self) -> None:
        """Terminate the peer connection."""
        logger.info(f'{self._log_prefix}: closing connection')
        await self._pc.close()

    async def send(self, data: bytes) -> None:
        """Send message to peer.

        Args:
            data (bytes): data to send to peer.
        """
        await self._handshake_complete.wait()
        self._channel.send(data)
        # https://github.com/aiortc/aiortc/issues/547
        await self._channel._RTCDataChannel__transport._data_channel_flush()
        await self._channel._RTCDataChannel__transport._transmit()
        logger.debug(f'{self._log_prefix}: sending message to peer')

    async def recv(self) -> bytes:
        """Receive next message from peer.

        Returns:
            bytes received from peer.
        """
        return await self._message_queue.get()

    async def send_offer(self, peer_uuid: str) -> None:
        """Send offer for peering via signaling server.

        Args:
            peer_uuid (str): uuid of peer client to establish connection with.
        """
        self._channel = self._pc.createDataChannel('p2p')

        @self._channel.on('open')
        def on_open() -> None:
            logger.info(f'{self._log_prefix}: peer channel established')
            self._handshake_complete.set()

        @self._channel.on('message')
        def on_message(message: bytes) -> None:
            logger.debug(f'{self._log_prefix}: received message from peer')
            self._message_queue.put_nowait(message)

        await self._pc.setLocalDescription(await self._pc.createOffer())
        message = serialize(
            PeerConnectionMessage(
                source_uuid=self._uuid,
                source_name=self._name,
                peer_uuid=peer_uuid,
                message=json.dumps(
                    {
                        'offer': object_to_string(self._pc.localDescription),
                    },
                ),
            ),
        )
        logger.info(f'{self._log_prefix}: sending offer to {peer_uuid}')
        await self._websocket.send(message)

    async def send_answer(self, peer_uuid: str) -> None:
        """Send answer to peering request via signaling server.

        Args:
            peer_uuid (str): uuid of peer client that sent the initial offer.
        """

        @self._pc.on('datachannel')
        def on_datachannel(channel: RTCDataChannel) -> None:
            logger.info(f'{self._log_prefix}: peer channel established')
            self._channel = channel
            self._handshake_complete.set()

            @channel.on('message')
            def on_message(message: bytes) -> None:
                logger.debug(f'{self._log_prefix}: received message from peer')
                self._message_queue.put_nowait(message)

        await self._pc.setLocalDescription(await self._pc.createAnswer())
        message = serialize(
            PeerConnectionMessage(
                source_uuid=self._uuid,
                source_name=self._name,
                peer_uuid=peer_uuid,
                message=json.dumps(
                    {
                        'offer': object_to_string(self._pc.localDescription),
                    },
                ),
            ),
        )
        logger.info(f'{self._log_prefix}: sending answer to {peer_uuid}')
        await self._websocket.send(message)

    async def handle_server_message(
        self,
        message: PeerConnectionMessage,
    ) -> None:
        """Handle message from the signaling server.

        Args:
            message (PeerConnectionMessage): message received from the
                signaling server.
        """
        if message.error is not None:
            raise message.error

        if message.message is None:
            raise AssertionError(
                'Received message from signaling server that has no '
                'message or error.',
            )
        message_data = json.loads(message.message)
        if 'offer' in message_data:
            logger.info(
                f'{self._log_prefix}: received offer from '
                f'{message.source_uuid} ({message.source_name})',
            )
            obj = object_from_string(message_data['offer'])
        elif 'answer' in message_data:  # pragma: no cover
            logger.info(
                f'{self._log_prefix}: received answer from '
                f'{message.source_uuid} ({message.source_name})',
            )
            obj = object_from_string(message_data['answer'])
        else:
            raise AssertionError(
                'P2P connection message does not contain either an offer or '
                'an answer',
            )

        if isinstance(obj, RTCSessionDescription):
            await self._pc.setRemoteDescription(obj)
            self._peer_uuid = message.source_uuid
            self._peer_name = message.source_name
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


def log_name(uuid: str, name: str) -> str:
    """Return str formatted as `name(uuid-prefix)`."""
    return f'{name}({uuid[:min(8,len(uuid))]})'
