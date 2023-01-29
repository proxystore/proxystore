"""Representation of peer-to-peer connection."""
from __future__ import annotations

import asyncio
import logging
import re
import warnings
from collections import defaultdict
from uuid import UUID

try:
    from aiortc import RTCDataChannel
    from aiortc import RTCIceCandidate
    from aiortc import RTCPeerConnection
    from aiortc import RTCSessionDescription
    from aiortc.contrib.signaling import BYE
    from aiortc.contrib.signaling import object_from_string
    from aiortc.contrib.signaling import object_to_string
    from cryptography.utils import CryptographyDeprecationWarning
    from websockets.client import WebSocketClientProtocol

    warnings.simplefilter('ignore', CryptographyDeprecationWarning)
except ImportError as e:  # pragma: no cover
    warnings.warn(
        f'{e}. To enable endpoint serving, install proxystore with '
        '"pip install proxystore[endpoints]".',
    )

from proxystore.p2p import messages
from proxystore.p2p.chunks import Chunk
from proxystore.p2p.chunks import chunkify
from proxystore.p2p.chunks import reconstruct
from proxystore.p2p.counter import AtomicCounter
from proxystore.p2p.exceptions import PeerConnectionError
from proxystore.p2p.exceptions import PeerConnectionTimeoutError

logger = logging.getLogger(__name__)

# These values were manually found using
# testing/scripts/peer_connection_bandwidth.py
MAX_CHUNK_SIZE_STRING = 2**15
MAX_CHUNK_SIZE_BYTES = 2**15


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
       from proxystore.p2p.messages import decode
       from proxystore.p2p.server import connect

       uuid1, name1, websocket1 = await connect(signaling_server_address)
       connection1 = PeerConnection(uuid1, name1, websocket1)

       uuid2, name2, websocket2 = await connect(signaling_server_address)
       connection2 = PeerConnection(uuid2, name2, websocket2)

       await connection1.send_offer(uuid2)
       offer = decode(await websocket2.recv())
       await connection2.handle_server_message(offer)
       answer = decode(await websocket1.recv())
       await connection1.handle_server_message(answer)

       await connection1.ready()
       await connection2.ready()

       await connection1.send('hello')
       assert await connection2.recv() == 'hello'
       await connection2.send('hello hello')
       assert await connection1.recv() == 'hello hello'

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
        *,
        channels: int = 1,
    ) -> None:
        """Init P2PConnection.

        Args:
            uuid (str): uuid of this client.
            name (str): readable name of this client for logging.
            websocket (WebSocketClientProtocol): websocket connection to the
                signaling server.
            channels (int): number of datachannels to open with peer
                (default: 1).
        """
        self._uuid = uuid
        self._name = name
        self._websocket = websocket
        self._max_channels = channels

        self._handshake_success: asyncio.Future[
            bool
        ] = asyncio.get_running_loop().create_future()
        self._pc = RTCPeerConnection()

        self._incoming_queue: asyncio.Queue[bytes | str] = asyncio.Queue()
        self._incoming_chunks: dict[int, list[Chunk]] = defaultdict(list)
        # Max size of unsigned long (4 bytes) is 2^32 - 1
        self._message_counter = AtomicCounter(size=2**32 - 1)

        # Used by offerer to count how many of the channels it opened are ready
        self._ready = 0
        self._channels: dict[str, RTCDataChannel] = {}
        self._channel_buffer_low: dict[str, asyncio.Event] = {}

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
        # Flush send buffers before close
        # https://github.com/aiortc/aiortc/issues/547
        for channel in self._channels.values():
            transport = channel._RTCDataChannel__transport
            await transport._data_channel_flush()
            await transport._transmit()
        await self._pc.close()

    async def send(self, message: bytes | str, timeout: float = 30) -> None:
        """Send message to peer.

        Args:
            message (bytes, str): message to send to peer.
            timeout (float): timeout to wait on peer connection to be ready.

        Raises:
            PeerConnectionTimeoutError:
                if the peer connection is not established within the timeout.
        """
        await self.ready(timeout)

        chunk_size = (
            MAX_CHUNK_SIZE_STRING
            if isinstance(message, str)
            else MAX_CHUNK_SIZE_BYTES
        )

        message_id = self._message_counter.increment()
        channel_names = list(self._channels.keys())

        for i, chunk in enumerate(chunkify(message, chunk_size, message_id)):
            channel_name = channel_names[i % len(channel_names)]
            channel = self._channels[channel_name]
            buffer_low = self._channel_buffer_low[channel_name]
            if channel.bufferedAmount > channel.bufferedAmountLowThreshold:
                await buffer_low.wait()
                buffer_low.clear()
            channel.send(bytes(chunk))

        logger.debug(f'{self._log_prefix}: sending message to peer')

    async def recv(self) -> bytes | str:
        """Receive next message from peer.

        Returns:
            message (string or bytes) received from peer.
        """
        return await self._incoming_queue.get()

    async def send_offer(self, peer_uuid: UUID) -> None:
        """Send offer for peering via signaling server.

        Args:
            peer_uuid (str): uuid of peer client to establish connection with.
        """
        for i in range(self._max_channels):
            label = f'p2p-{i}-{self._max_channels}'
            channel = self._pc.createDataChannel(label, ordered=False)
            buffer_low = asyncio.Event()
            channel.on('open', self._on_open)
            channel.on('bufferedamountlow', buffer_low.set)
            channel.on('message', self._on_message)
            self._channels[label] = channel
            self._channel_buffer_low[label] = buffer_low

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
            # TODO: note this is first channel opened
            match = re.search(r'(\d+)-(\d+)$', channel.label)
            if match is None:
                raise AssertionError(
                    f'Got mislabled datachannel {channel.label}',
                )
            total = int(match.group(2))

            buffer_low = asyncio.Event()
            self._channels[channel.label] = channel
            self._channel_buffer_low[channel.label] = buffer_low
            channel.on('bufferedamountlow', buffer_low.set)
            channel.on('message', self._on_message)

            if len(self._channels) >= total:
                self._handshake_success.set_result(True)

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

    async def _on_message(self, data: bytes) -> None:
        chunk = Chunk.from_bytes(data)
        self._incoming_chunks[chunk.stream_id].append(chunk)

        if len(self._incoming_chunks[chunk.stream_id]) == chunk.seq_len:
            chunks = self._incoming_chunks.pop(chunk.stream_id)
            message = reconstruct(chunks)
            await self._incoming_queue.put(message)
            logger.debug(f'{self._log_prefix}: received message from peer')

    def _on_open(self) -> None:
        # Note: this callback is only used on the offerer/initiators side
        logger.info(f'{self._log_prefix}: peer channels established')
        self._ready += 1
        if self._ready >= self._max_channels:
            self._handshake_success.set_result(True)

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
            PeerConnectionTimeoutError:
                if the connection is not ready within the timeout.
            PeerConnectionError:
                if there is an error establishing the peer connection.
        """
        try:
            await asyncio.wait_for(self._handshake_success, timeout)
        except asyncio.TimeoutError as e:
            raise PeerConnectionTimeoutError(
                'Timeout waiting for peer to peer connection to establish '
                f'in {self._log_prefix}.',
            ) from e


def log_name(uuid: UUID, name: str) -> str:
    """Return str formatted as `name(uuid-prefix)`."""
    uuid_ = str(uuid)
    return f'{name}({uuid_[:min(8,len(uuid_))]})'
