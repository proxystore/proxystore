"""Representation of peer-to-peer connection."""
from __future__ import annotations

import asyncio
import logging
import warnings
from uuid import UUID

from aiortc import RTCDataChannel
from aiortc import RTCIceCandidate
from aiortc import RTCPeerConnection
from aiortc import RTCSessionDescription
from aiortc.contrib.signaling import BYE
from aiortc.contrib.signaling import object_from_string
from aiortc.contrib.signaling import object_to_string
from cryptography.utils import CryptographyDeprecationWarning
from websockets.client import WebSocketClientProtocol

from proxystore.p2p import messages
from proxystore.p2p.exceptions import PeerConnectionError
from proxystore.p2p.exceptions import PeerConnectionTimeout

logger = logging.getLogger(__name__)
warnings.simplefilter('ignore', CryptographyDeprecationWarning)


class PeerConnection:
    """Peer-to-peer connection.

    Interface for establishing a peer-to-peer connection via WebRTC
    (`aiortc <https://aiortc.readthedocs.io/en/latest/>`_) and
    sending/receiving messages between the two peers. The peer-to-peer
    connection is established using a central and publicly accessible
    signaling server.

    Warning:
        Applications should prefer using the
        :any:`PeerManager <proxystore.p2p.manager.PeerManager>` rather than
        using the :class:`PeerConnection <PeerConnection>` class.

    .. code-block:: python

       from proxystore.p2p.connection import PeerConnection
       from proxystore.p2p.server import connect
       from proxystore.serialize import deserialize

       uuid1, name1, websocket1 = await connect(signaling_server.address)
       connection1 = PeerConnection(uuid1, name1, websocket1)

       uuid2, name2, websocket2 = await connect(signaling_server.address)
       connection2 = PeerConnection(uuid2, name2, websocket2)

       await connection1.send_offer(uuid2)
       offer = deserialize(await websocket2.recv())
       await connection2.handle_server_message(offer)
       answer = deserialize(await websocket1.recv())
       await connection1.handle_server_message(answer)

       await connection1.ready()
       await connection2.ready()

       await connection1.send(b'hello')
       assert await connection2.recv() == b'hello'
       await connection2.send(b'hello hello')
       assert await connection1.recv() == b'hello hello'

       await websocket1.close()
       await websocket2.close()
       await connection1.close()
       await connection2.close()
    """

    def __init__(
        self,
        uuid: UUID,
        name: str,
        websocket: WebSocketClientProtocol,
    ) -> None:
        """Init P2PConnection.

        Args:
            uuid (str): uuid of this client.
            name (str): readable name of this client for logging.
            websocket (WebSocketClientProtocol): websocket connection to the
                signaling server.
        """
        self._uuid = uuid
        self._name = name
        self._websocket = websocket

        self._handshake_success: asyncio.Future[bool] = asyncio.Future()
        self._pc = RTCPeerConnection()
        self._message_queue: asyncio.Queue[str] = asyncio.Queue()

        self._peer_uuid: UUID | None = None
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

    async def send(self, message: str, timeout: float = 30) -> None:
        """Send message to peer.

        Args:
            message (str): message to send to peer.
            timeout (float): timeout to wait on peer connection to be ready.

        Raises:
            PeerConnectionTimeout:
                if the peer connection is not established within the timeout.
        """
        await self.ready(timeout)
        self._channel.send(message)
        # https://github.com/aiortc/aiortc/issues/547
        await self._channel._RTCDataChannel__transport._data_channel_flush()
        await self._channel._RTCDataChannel__transport._transmit()
        logger.debug(f'{self._log_prefix}: sending message to peer')

    async def recv(self) -> str:
        """Receive next message from peer.

        Returns:
            str received from peer.
        """
        return await self._message_queue.get()

    async def send_offer(self, peer_uuid: UUID) -> None:
        """Send offer for peering via signaling server.

        Args:
            peer_uuid (str): uuid of peer client to establish connection with.
        """
        self._channel = self._pc.createDataChannel('p2p')

        @self._channel.on('open')
        def on_open() -> None:
            logger.info(f'{self._log_prefix}: peer channel established')
            self._handshake_success.set_result(True)

        @self._channel.on('message')
        def on_message(message: str) -> None:
            logger.debug(f'{self._log_prefix}: received message from peer')
            self._message_queue.put_nowait(message)

        await self._pc.setLocalDescription(await self._pc.createOffer())
        message = messages.PeerConnection(
            source_uuid=self._uuid,
            source_name=self._name,
            peer_uuid=peer_uuid,
            description_type='offer',
            description=object_to_string(self._pc.localDescription),
        )
        message_str = messages.encode(message)
        logger.info(f'{self._log_prefix}: sending offer to {peer_uuid}')
        await self._websocket.send(message_str)

    async def send_answer(self, peer_uuid: UUID) -> None:
        """Send answer to peering request via signaling server.

        Args:
            peer_uuid (str): uuid of peer client that sent the initial offer.
        """

        @self._pc.on('datachannel')
        def on_datachannel(channel: RTCDataChannel) -> None:
            logger.info(f'{self._log_prefix}: peer channel established')
            self._channel = channel
            self._handshake_success.set_result(True)

            @channel.on('message')
            def on_message(message: str) -> None:
                logger.debug(f'{self._log_prefix}: received message from peer')
                self._message_queue.put_nowait(message)

        await self._pc.setLocalDescription(await self._pc.createAnswer())
        message = messages.PeerConnection(
            source_uuid=self._uuid,
            source_name=self._name,
            peer_uuid=peer_uuid,
            description_type='answer',
            description=object_to_string(self._pc.localDescription),
        )
        message_str = messages.encode(message)
        logger.info(f'{self._log_prefix}: sending answer to {peer_uuid}')
        await self._websocket.send(message_str)

    async def handle_server_message(
        self,
        message: messages.PeerConnection,
    ) -> None:
        """Handle message from the signaling server.

        Args:
            message (PeerConnection): message received from the
                signaling server.
        """
        if message.error is not None:
            self._handshake_success.set_exception(
                PeerConnectionError(
                    'Received error message from signaling server: '
                    f'{str(message.error)}',
                ),
            )
            return

        if message.description_type == 'offer':
            logger.info(
                f'{self._log_prefix}: received offer from '
                f'{message.source_uuid} ({message.source_name})',
            )
            obj = object_from_string(message.description)
        elif message.description_type == 'answer':
            logger.info(
                f'{self._log_prefix}: received answer from '
                f'{message.source_uuid} ({message.source_name})',
            )
            obj = object_from_string(message.description)
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

    async def ready(self, timeout: float | None = None) -> None:
        """Wait for connection to be ready.

        Args:
            timeout (float, optional): maximum time in seconds to wait for
                the peer connection to establish. If None, block until
                the connection is established (default: None).

        Raises:
            PeerConnectionTimeout:
                if the connection is not ready within the timeout.
            PeerConnectionError:
                if there is an error establishing the peer connection.
        """
        try:
            await asyncio.wait_for(self._handshake_success, timeout)
        except asyncio.TimeoutError:
            raise PeerConnectionTimeout(
                'Timeout waiting for peer to peer connection to establish '
                f'in {self._log_prefix}.',
            )


def log_name(uuid: UUID, name: str) -> str:
    """Return str formatted as `name(uuid-prefix)`."""
    uuid_ = str(uuid)
    return f'{name}({uuid_[:min(8,len(uuid_))]})'
