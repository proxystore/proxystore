"""Representation of peer-to-peer connection."""
from __future__ import annotations

import asyncio
import logging
import re
import warnings
from collections import defaultdict
from typing import Any
from typing import Awaitable
from typing import Callable
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

    warnings.simplefilter('ignore', CryptographyDeprecationWarning)
except ImportError as e:  # pragma: no cover
    warnings.warn(
        f'{e}. To enable endpoint serving, install proxystore with '
        '"pip install proxystore[endpoints]".',
        stacklevel=2,
    )

from proxystore.p2p.chunks import Chunk
from proxystore.p2p.chunks import chunkify
from proxystore.p2p.chunks import reconstruct
from proxystore.p2p.exceptions import PeerConnectionError
from proxystore.p2p.exceptions import PeerConnectionTimeoutError
from proxystore.p2p.relay.client import RelayClient
from proxystore.p2p.relay.messages import PeerConnectionRequest
from proxystore.utils.counter import AtomicCounter

logger = logging.getLogger(__name__)

# These values were manually found using
# testing/scripts/peer_connection_bandwidth.py
MAX_CHUNK_SIZE_STRING = 2**15
MAX_CHUNK_SIZE_BYTES = 2**15


class PeerConnection:
    """Peer-to-peer connection.

    Interface for establishing a peer-to-peer connection via WebRTC
    [aiortc](https://aiortc.readthedocs.io/en/latest/){target=_blank} and
    sending/receiving messages between the two peers. The peer-to-peer
    connection is established using a central and publicly accessible
    relay server.

    Warning:
        Applications should prefer using the
        [`PeerManager`][proxystore.p2p.manager.PeerManager] rather than using
        the [`PeerConnection`][proxystore.p2p.connection.PeerConnection] class.

    Example:
        ```python
        from proxystore.p2p.connection import PeerConnection
        from proxystore.p2p.relay import BasicRelayClient

        client1 = BasicRelayClient(relay_server_address)
        await client1.connect()
        connection1 = PeerConnection(client1)

        client2 = BasicRelayClient(relay_server_address)
        await client2.connect()
        connection2 = PeerConnection(client2)

        await connection1.send_offer(client2.uuid)
        offer = await client2.recv()
        await connection2.handle_server_message(offer)
        answer = await client1.recv()
        await connection1.handle_server_message(answer)

        await connection1.ready()
        await connection2.ready()

        await connection1.send('hello')
        assert await connection2.recv() == 'hello'
        await connection2.send('hello hello')
        assert await connection1.recv() == 'hello hello'

        await client1.close()
        await client2.close()
        await connection1.close()
        await connection2.close()
        ```

    Args:
        relay_client: Client connection to the relay server.
        channels: Number of datachannels to open with peer.
    """

    def __init__(
        self,
        relay_client: RelayClient,
        *,
        channels: int = 1,
    ) -> None:
        self._relay_client = relay_client
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
        local = log_name(self._relay_client.uuid, self._relay_client.name)
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
            One of 'connected', 'connecting', 'closed', 'failed', or 'new'.
        """
        return self._pc.connectionState

    async def close(self) -> None:
        """Terminate the peer connection.

        Note:
            This will not call
            [`RelayClient.close()`][proxystore.p2p.relay.client.RelayClient].
        """
        logger.info(f'{self._log_prefix}: closing connection')
        # Flush send buffers before close
        # https://github.com/aiortc/aiortc/issues/547
        for channel in self._channels.values():
            transport = channel._RTCDataChannel__transport
            await transport._data_channel_flush()
            await transport._transmit()
            channel.close()
        await self._pc.close()

    def on_close_callback(
        self,
        callback: Callable[..., Awaitable[None]],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Configure a callback for when the connection fails or closes.

        Args:
            callback: Callable to invoke when the peer connection state
                changes to closed or failed.
            args: Positional arguments to pass to the callback.
            kwargs: Keyword arguments to pass to the callback.
        """

        async def _on_close() -> None:
            if self.state in ('closed', 'failed'):
                logger.info(
                    f'{self._log_prefix}: connection entered {self.state} '
                    'state, invoking callback',
                )
                await callback(*args, **kwargs)

        self._pc.on('connectionstatechange', _on_close)

    async def send(self, message: bytes | str, timeout: float = 30) -> None:
        """Send message to peer.

        Args:
            message: Message to send to peer.
            timeout: Timeout to wait on peer connection to be ready.

        Raises:
            PeerConnectionTimeoutError: If the peer connection is not
                established within the timeout.
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
            Message received from peer.
        """
        return await self._incoming_queue.get()

    async def send_offer(self, peer_uuid: UUID) -> None:
        """Send offer for peering via relay server.

        Args:
            peer_uuid: UUID of peer client to establish connection with.
        """

        def _on_close(label: str) -> Any:
            # We use this factory method to avoid Flake8-BugBear B023
            async def on_close() -> None:
                if self._channels[label].readyState in ('closed', 'failed'):
                    await self.close()

            return on_close

        for i in range(self._max_channels):
            label = f'p2p-{i}-{self._max_channels}'
            channel = self._pc.createDataChannel(label, ordered=False)
            buffer_low = asyncio.Event()
            channel.on('open', self._on_datachannel_open)
            channel.on('bufferedamountlow', buffer_low.set)
            channel.on('message', self._on_message)

            self._channels[label] = channel
            self._channel_buffer_low[label] = buffer_low

            # We use the underlying RTCDtlsTransport as the channel status.
            channel.transport.transport.on('statechange', _on_close(label))

        await self._pc.setLocalDescription(await self._pc.createOffer())
        message = PeerConnectionRequest(
            source_uuid=self._relay_client.uuid,
            source_name=self._relay_client.name,
            peer_uuid=peer_uuid,
            description_type='offer',
            description=object_to_string(self._pc.localDescription),
        )
        logger.info(f'{self._log_prefix}: sending offer to {peer_uuid}')
        await self._relay_client.send(message)

    async def send_answer(self, peer_uuid: UUID) -> None:
        """Send answer to peering request via relay server.

        Args:
            peer_uuid: UUID of peer client that sent the initial offer.
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

            async def _on_close() -> None:
                if channel.readyState in ('closed', 'failed'):
                    await self.close()
                else:
                    pass  # pragma: no cover

            # We use the underlying RTCDtlsTransport as the channel status
            channel.transport.transport.on('statechange', _on_close)

            if len(self._channels) >= total:
                self._handshake_success.set_result(True)

        await self._pc.setLocalDescription(await self._pc.createAnswer())
        message = PeerConnectionRequest(
            source_uuid=self._relay_client.uuid,
            source_name=self._relay_client.name,
            peer_uuid=peer_uuid,
            description_type='answer',
            description=object_to_string(self._pc.localDescription),
        )
        logger.info(f'{self._log_prefix}: sending answer to {peer_uuid}')
        await self._relay_client.send(message)

    async def _on_message(self, data: bytes) -> None:
        chunk = Chunk.from_bytes(data)
        self._incoming_chunks[chunk.stream_id].append(chunk)

        if len(self._incoming_chunks[chunk.stream_id]) == chunk.seq_len:
            chunks = self._incoming_chunks.pop(chunk.stream_id)
            message = reconstruct(chunks)
            await self._incoming_queue.put(message)
            logger.debug(f'{self._log_prefix}: received message from peer')

    def _on_datachannel_open(self) -> None:
        # Note: this callback is only used on the offerer/initiators side
        logger.info(f'{self._log_prefix}: peer channels established')
        self._ready += 1
        if self._ready >= self._max_channels:
            self._handshake_success.set_result(True)

    async def handle_server_message(
        self,
        message: PeerConnectionRequest,
    ) -> None:
        """Handle message from the relay server.

        Args:
            message: Message received from the relay server.
        """
        if message.error is not None:
            self._handshake_success.set_exception(
                PeerConnectionError(
                    'Received error message from relay server: '
                    f'{message.error!s}',
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
            # relay server but this is here following the aiortc example.
            # https://github.com/aiortc/aiortc/blob/713fb644b95328f8ec1ac2cbb54def0424cc6645/examples/datachannel-cli/cli.py#L30  # noqa: E501
            await self._pc.addIceCandidate(obj)
        elif obj is BYE:  # pragma: no cover
            raise AssertionError('received BYE message')
        else:
            raise AssertionError('received unknown message')

    async def ready(self, timeout: float | None = None) -> None:
        """Wait for connection to be ready.

        Args:
            timeout: The maximum time in seconds to wait for
                the peer connection to establish. If None, block until
                the connection is established.

        Raises:
            PeerConnectionTimeoutError: If the connection is not ready within
                the timeout.
            PeerConnectionError: If there is an error establishing the peer
                connection.
        """
        try:
            await asyncio.wait_for(self._handshake_success, timeout)
        except asyncio.TimeoutError as e:
            raise PeerConnectionTimeoutError(
                'Timeout waiting for peer to peer connection to establish '
                f'in {self._log_prefix}.',
            ) from e


def log_name(uuid: UUID, name: str) -> str:
    """Return string formatted as `#!python 'name(uuid-prefix)'`."""
    uuid_ = str(uuid)
    return f'{name}({uuid_[:min(8,len(uuid_))]})'
