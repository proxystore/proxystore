"""Manager of peer connections to other endpoints."""

from __future__ import annotations

import asyncio
import collections
import contextlib
import dataclasses
import enum
import logging
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Coroutine
from typing import Any
from typing import Self

try:
    import iroh
except ImportError as e:  # pragma: no cover
    raise ImportError(
        f'{e}. To enable endpoint peering, install proxystore with '
        '"pip install proxystore[endpoints]".',
    ) from e

from proxystore.endpoint.config import EndpointP2PConfig
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.exceptions import PeerConnectionError
from proxystore.endpoint.exceptions import PeerConnectionTimeoutError
from proxystore.endpoint.exceptions import PeerNotAllowedError
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.identity import SecretKey
from proxystore.endpoint.peers import Allowlist
from proxystore.endpoint.protocol import decode_meta
from proxystore.endpoint.protocol import Header
from proxystore.endpoint.protocol import pack_message
from proxystore.endpoint.protocol import Status
from proxystore.p2p.addrs import PeerAddrCache
from proxystore.serialize import BytesLike
from proxystore.utils.tasks import spawn_guarded_background_task

logger = logging.getLogger(__name__)

ALPN = b'proxystore/1'
"""Application protocol negotiated on connections between endpoints.

Increment the version on any incompatible change to the messages exchanged
between endpoints.
"""

_CHUNK_SIZE = 64 * 1024 * 1024
# The iroh bindings limit the size of a single read to a u32, and each write
# copies the data, so data is read and written in chunks.

_STOP_REJECTED = 1
# Error code used to stop reading a request which is rejected.

PeerMessage = tuple[int, dict[str, Any], bytes | bytearray]
"""Code, metadata, and data of a message exchanged between peers."""

RequestHandler = Callable[
    [EndpointId, int, dict[str, Any], bytes | bytearray],
    Awaitable[tuple[int, dict[str, Any] | None, BytesLike | None]],
]
"""Handler of requests from peers.

The handler is called with the ID of the peer and the op code, metadata, and
data of the request and returns the status code, metadata, and data of the
response.
"""


_PATH_WATCH_INTERVAL = 1.0
_PATH_WATCH_DURATION = 15.0
# A new connection often starts relayed and switches to a direct path once
# hole-punching succeeds, so the path is watched for a short time after
# connecting to log the change even if the connection is idle.


@dataclasses.dataclass(frozen=True)
class PathInfo:
    """Network path used by a connection to a peer.

    Attributes:
        relayed: If traffic is relayed rather than sent directly to the peer.
        remote_addr: Address of the peer (or relay) on this path.
        rtt_ms: Round-trip time in milliseconds estimated by QUIC.
    """

    relayed: bool
    remote_addr: str
    rtt_ms: int

    @classmethod
    def from_connection(cls, connection: iroh.Connection) -> Self | None:
        """Get the path selected for sending on a connection.

        Returns:
            The selected path or `None` if the connection has no path.
        """
        for path in connection.paths():
            if path.is_selected:
                return cls(
                    relayed=path.is_relay,
                    remote_addr=path.remote_addr,
                    rtt_ms=path.rtt_ms,
                )
        return None

    def describe(self) -> str:
        """Describe the path for logging."""
        kind = 'relayed via' if self.relayed else 'direct to'
        return f'{kind} {self.remote_addr} (rtt {self.rtt_ms} ms)'


def relay_options(
    config: EndpointP2PConfig,
) -> tuple[iroh.Preset, iroh.RelayMode | None]:
    """Get the iroh preset and relay mode for a peer-to-peer configuration.

    Returns:
        The preset and relay mode (`None` uses the relays of the preset).
    """
    if config.relays == 'n0':
        return iroh.preset_n0(), None
    if config.relays == 'none':
        return iroh.preset_n0(), iroh.RelayMode.disabled()
    return iroh.preset_n0(), iroh.RelayMode.custom_from_urls(config.relays)


class CloseCode(enum.IntEnum):
    """Application error codes used when closing a peer connection."""

    SHUTDOWN = 0
    """Endpoint is shutting down."""
    NOT_ALLOWED = 1
    """Peer is not in the allowlist of the endpoint."""


class _DataTooLargeError(Exception):
    pass


class PeerManager:
    """Manager of connections to peer endpoints.

    The manager binds an iroh endpoint using the secret key of the ProxyStore
    endpoint, accepts connections from peers, and sends requests to peers.
    Each request is sent on its own bidirectional stream over a single
    connection to each peer.

    The manager only communicates with peers in the allowlist. Connections
    from other endpoints are refused, and requests to other endpoints fail
    with
    [`PeerNotAllowedError`][proxystore.endpoint.exceptions.PeerNotAllowedError].
    The allowlist is checked for changes on each connection and request, and
    connections to peers removed from the allowlist are closed.

    Example:
        ```python
        manager = PeerManager(secret_key, Allowlist(endpoint_dir.peers_path))
        await manager.start(handler)
        status, meta, data = await manager.request(peer_id, Op.GET, meta)
        await manager.close()
        ```

    Args:
        secret_key: Secret key of the endpoint.
        allowlist: Allowlist of peer endpoints.
        preset: iroh preset used to configure discovery and relays. Defaults
            to `iroh.preset_n0()` which uses n0's public
            relays and DNS discovery.
        relay_mode: Optional relay mode which overrides the relays of the
            preset.
        bind_addr: Optional address to bind to (e.g., `"127.0.0.1:0"`).
        connect_timeout: Timeout in seconds when connecting to a peer.
        online_timeout: Timeout in seconds to wait for the endpoint to
            connect to its home relay before logging a warning. If `None`,
            the endpoint does not wait (e.g., because relays are disabled).
        max_request_size: Maximum size in bytes of the data in a request from
            a peer or `None` for no limit.
        addr_cache: Optional cache where the addresses of peers are saved
            (see [`PeerAddrCache`][proxystore.p2p.addrs.PeerAddrCache]).
            Cached addresses are used when connecting to peers so peers can
            be reached even if discovery is unavailable.
    """

    def __init__(
        self,
        secret_key: SecretKey,
        allowlist: Allowlist,
        *,
        preset: iroh.Preset | None = None,
        relay_mode: iroh.RelayMode | None = None,
        bind_addr: str | None = None,
        connect_timeout: float = 30,
        online_timeout: float | None = 10,
        max_request_size: int | None = None,
        addr_cache: PeerAddrCache | None = None,
    ) -> None:
        self._secret_key = secret_key
        self._id = secret_key.endpoint_id
        self._allowlist = allowlist
        self._preset = preset
        self._relay_mode = relay_mode
        self._bind_addr = bind_addr
        self._connect_timeout = connect_timeout
        self._online_timeout = online_timeout
        self._max_request_size = max_request_size
        self._addr_cache = addr_cache

        self._endpoint: iroh.Endpoint | None = None
        self._handler: RequestHandler | None = None
        self._accept_task: asyncio.Task[None] | None = None
        self._online_task: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[None]] = set()

        self._addr_hints: dict[EndpointId, iroh.EndpointAddr] = {}
        self._dial_locks: collections.defaultdict[
            EndpointId,
            asyncio.Lock,
        ] = collections.defaultdict(asyncio.Lock)
        self._outgoing: dict[EndpointId, iroh.Connection] = {}
        self._incoming: dict[EndpointId, set[iroh.Connection]] = {}
        # Last reported path of each connection by stable ID.
        self._paths: dict[int, tuple[bool, str] | None] = {}
        self._closed = False

    @classmethod
    def from_endpoint_dir(
        cls,
        endpoint_dir: EndpointDir,
        **options: Any,
    ) -> Self:
        """Create a peer manager for an endpoint.

        The secret key, peers, relays, maximum object size, and address
        cache are taken from the endpoint directory and configuration.

        Args:
            endpoint_dir: Directory of the endpoint.
            options: Options which override the defaults from the
                configuration (see
                [`PeerManager`][proxystore.p2p.manager.PeerManager]).

        Raises:
            FileNotFoundError: If the configuration or secret key does not
                exist.
            ValueError: If the configuration is invalid or does not match
                the secret key.
        """
        config = endpoint_dir.read_config()
        preset, relay_mode = relay_options(config.p2p)
        defaults: dict[str, Any] = {
            'preset': preset,
            'relay_mode': relay_mode,
            # Without relays, there is no home relay to wait on.
            'online_timeout': None if config.p2p.relays == 'none' else 10,
            'max_request_size': config.storage.object_size_limit,
            'addr_cache': PeerAddrCache(endpoint_dir.peer_addrs_path),
        }
        return cls(
            endpoint_dir.read_secret_key(),
            endpoint_dir.peers.allowlist(),
            **{**defaults, **options},
        )

    @property
    def id(self) -> EndpointId:
        """ID of this endpoint."""
        return self._id

    @property
    def endpoint(self) -> iroh.Endpoint:
        """Underlying iroh endpoint.

        Raises:
            RuntimeError: If the manager has not been started.
        """
        if self._endpoint is None:
            raise RuntimeError('The peer manager has not been started.')
        return self._endpoint

    def path(self, peer_id: EndpointId) -> PathInfo | None:
        """Get the path used by the connection to a peer.

        Returns:
            The path of the connection this endpoint opened to the peer or \
            `None` if there is no open connection.
        """
        connection = self._outgoing.get(peer_id)
        if connection is None or connection.close_reason() is not None:
            return None
        return PathInfo.from_connection(connection)

    def addr(self) -> iroh.EndpointAddr:
        """Get the current address of this endpoint.

        The address contains the ID, home relay URL, and direct addresses
        of the endpoint. Other endpoints can use the address to connect to
        this endpoint without discovery (see
        [`add_peer_addr()`][proxystore.p2p.manager.PeerManager.add_peer_addr]).
        """
        return self.endpoint.addr()

    def add_peer_addr(self, addr: iroh.EndpointAddr) -> None:
        """Add a known address of a peer to use when connecting to the peer.

        This is useful when discovery is unavailable.
        """
        peer_id = EndpointId.from_str(str(addr.id()))
        self._addr_hints[peer_id] = addr

    def _log_prefix(self) -> str:
        return f'{type(self).__name__}[{self.id.log_name("self")}]'

    def _peer_name(self, peer_id: EndpointId) -> str:
        name = self._allowlist.name_of(peer_id)
        return peer_id.log_name('unknown' if name is None else name)

    async def start(self, handler: RequestHandler) -> None:
        """Bind the endpoint and start accepting connections from peers.

        Note:
            The iroh bindings run background threads so the manager must be
            started after the process is daemonized or forked.

        Args:
            handler: Handler of requests from peers.
        """
        if self._endpoint is not None:
            return
        self._handler = handler
        if self._addr_cache is not None:
            cached = self._addr_cache.load()
            for peer_id, addr in cached.items():
                self._addr_hints.setdefault(peer_id, addr)
            logger.info(
                '%s: loaded %d cached peer address(es) from %s',
                self._log_prefix(),
                len(cached),
                self._addr_cache.path,
            )
        # uniffi_set_event_loop() is intentionally not called. It sets a
        # process-wide event loop that the bindings then use for every call,
        # which breaks when a different event loop is used later. It is only
        # needed for callbacks from Rust into Python which are not used.
        options = iroh.EndpointOptions(
            preset=iroh.preset_n0() if self._preset is None else self._preset,
            secret_key=self._secret_key.to_bytes(),
            alpns=[ALPN],
            relay_mode=self._relay_mode,
            bind_addr=self._bind_addr,
        )
        self._endpoint = await iroh.Endpoint.bind(options)
        self._accept_task = spawn_guarded_background_task(self._accept_loop)
        self._accept_task.set_name(f'peer-manager-{self.id}-accept')
        if self._online_timeout is not None:
            self._online_task = asyncio.create_task(
                self._wait_online(self._online_timeout),
            )
        logger.info(
            '%s: listening for peer connections on %s',
            self._log_prefix(),
            ', '.join(self.endpoint.bound_sockets()),
        )

    async def close(self) -> None:
        """Close all peer connections and the endpoint.

        This is idempotent so it is safe to call multiple times.
        """
        if self._closed:
            return
        self._closed = True
        for task in (self._accept_task, self._online_task, *self._tasks):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        for peer_id in {*self._outgoing, *self._incoming}:
            self._close_peer(peer_id, CloseCode.SHUTDOWN, b'shutdown')
        if self._endpoint is not None:
            await self._endpoint.close()
        logger.info('%s: peer manager closed', self._log_prefix())

    async def request(
        self,
        peer_id: EndpointId,
        code: int,
        meta: dict[str, Any] | None = None,
        data: BytesLike | None = None,
    ) -> PeerMessage:
        """Send a request to a peer and wait for the response.

        Args:
            peer_id: ID of the peer.
            code: Op code of the request.
            meta: Metadata of the request.
            data: Data of the request.

        Returns:
            Status code, metadata, and data of the response.

        Raises:
            PeerNotAllowedError: If the peer is not in the allowlist or the
                peer refused the connection.
            PeerConnectionTimeoutError: If connecting to the peer times out.
            PeerConnectionError: If the request fails.
        """
        if not self._is_allowed(peer_id):
            raise PeerNotAllowedError(
                f'Endpoint {peer_id} is not in the allowlist of peers. Add '
                'the endpoint with "proxystore-endpoint peers add".',
            )

        for attempt in range(2):
            connection, fresh = await self._get_connection(peer_id)
            try:
                response = await self._exchange(connection, code, meta, data)
                self._report_path(peer_id, connection, outgoing=True)
                return response
            except iroh.IrohError as e:
                reason = connection.close_reason()
                self._drop_outgoing(peer_id, connection)
                if reason is not None and _closed_with(
                    reason,
                    CloseCode.NOT_ALLOWED,
                ):
                    raise PeerNotAllowedError(
                        f'Peer {peer_id} refused the connection because '
                        'this endpoint is not in its allowlist of peers.',
                    ) from None
                # A cached connection may have been closed (e.g., because
                # the peer restarted) so retry once with a new connection.
                if fresh or attempt > 0:
                    raise PeerConnectionError(
                        f'Request to peer {peer_id} failed: {_message(e)}',
                    ) from None
                logger.debug(
                    '%s: retrying request to %s with a new connection: %s',
                    self._log_prefix(),
                    self._peer_name(peer_id),
                    _message(e),
                )
        raise AssertionError('Unreachable.')

    async def _exchange(
        self,
        connection: iroh.Connection,
        code: int,
        meta: dict[str, Any] | None,
        data: BytesLike | None,
    ) -> PeerMessage:
        stream = await connection.open_bi()
        try:
            await _write_message(stream.send(), code, meta, data)
        except iroh.IrohError as e:
            # The peer stops reading a request it rejects (e.g., because the
            # data is too large) but still sends a response with the reason.
            write_error: iroh.IrohError | None = e
        else:
            write_error = None
        try:
            header, response_meta, response_data = await _read_message(
                stream.recv(),
                max_data_size=None,
            )
        except iroh.IrohError:
            if write_error is not None:
                raise write_error from None
            raise
        return header.code, response_meta, response_data

    async def _get_connection(
        self,
        peer_id: EndpointId,
    ) -> tuple[iroh.Connection, bool]:
        async with self._dial_locks[peer_id]:
            connection = self._outgoing.get(peer_id)
            if connection is not None and connection.close_reason() is None:
                return connection, False

            logger.info(
                '%s: connecting to peer %s',
                self._log_prefix(),
                self._peer_name(peer_id),
            )
            id_only = iroh.EndpointAddr(
                iroh.EndpointId.from_string(peer_id),
                None,
                [],
            )
            hint = self._addr_hints.get(peer_id)
            try:
                connection = await self._dial(
                    peer_id,
                    id_only if hint is None else hint,
                )
            except PeerConnectionTimeoutError:
                raise
            except PeerConnectionError as e:
                if hint is None:
                    raise
                # The cached address may be stale so try again using only
                # discovery.
                logger.info(
                    '%s: failed to connect to peer %s using its cached '
                    'address, retrying with discovery: %s',
                    self._log_prefix(),
                    self._peer_name(peer_id),
                    e,
                )
                self._addr_hints.pop(peer_id, None)
                connection = await self._dial(peer_id, id_only)
            self._outgoing[peer_id] = connection
            logger.info(
                '%s: connected to peer %s',
                self._log_prefix(),
                self._peer_name(peer_id),
            )
            await self._remember_addr(peer_id)
            self._watch_path(peer_id, connection, outgoing=True)
            return connection, True

    async def _dial(
        self,
        peer_id: EndpointId,
        addr: iroh.EndpointAddr,
    ) -> iroh.Connection:
        try:
            return await asyncio.wait_for(
                self.endpoint.connect(addr, ALPN),
                timeout=self._connect_timeout,
            )
        except TimeoutError:
            raise PeerConnectionTimeoutError(
                f'Connecting to peer {peer_id} timed out after '
                f'{self._connect_timeout} seconds.',
            ) from None
        except iroh.IrohError as e:
            raise PeerConnectionError(
                f'Failed to connect to peer {peer_id}: {_message(e)}',
            ) from None

    async def _remember_addr(self, peer_id: EndpointId) -> None:
        addr = await self.endpoint.remote_addr(
            # The ID is a valid public key because the peer is connected.
            iroh.EndpointId.from_string(peer_id),
        )
        if addr is None:  # pragma: no cover
            return
        self._addr_hints[peer_id] = addr
        if self._addr_cache is not None:
            # Only peers in the allowlist are saved so removed peers are
            # pruned from the cache.
            addrs = {
                peer: addr
                for peer, addr in self._addr_hints.items()
                if self._allowlist.allowed(peer)
            }
            try:
                self._addr_cache.save(addrs)
            except OSError as e:
                logger.warning(
                    '%s: failed to save peer address cache: %s',
                    self._log_prefix(),
                    e,
                )

    def _drop_outgoing(
        self,
        peer_id: EndpointId,
        connection: iroh.Connection,
    ) -> None:
        self._paths.pop(connection.stable_id(), None)
        if self._outgoing.get(peer_id) is connection:
            del self._outgoing[peer_id]

    def _report_path(
        self,
        peer_id: EndpointId,
        connection: iroh.Connection,
        *,
        outgoing: bool,
    ) -> None:
        """Log the path of a connection if it changed."""
        direction = 'connection to' if outgoing else 'connection from'
        path = PathInfo.from_connection(connection)
        key = None if path is None else (path.relayed, path.remote_addr)
        stable_id = connection.stable_id()
        if stable_id in self._paths and self._paths[stable_id] == key:
            return
        self._paths[stable_id] = key
        if path is None:
            logger.info(
                '%s: %s peer %s has no path',
                self._log_prefix(),
                direction,
                self._peer_name(peer_id),
            )
        else:
            logger.info(
                '%s: %s peer %s is %s',
                self._log_prefix(),
                direction,
                self._peer_name(peer_id),
                path.describe(),
            )

    def _watch_path(
        self,
        peer_id: EndpointId,
        connection: iroh.Connection,
        *,
        outgoing: bool,
    ) -> None:
        self._report_path(peer_id, connection, outgoing=outgoing)
        self._spawn(
            self._watch_path_changes(peer_id, connection, outgoing=outgoing),
        )

    async def _watch_path_changes(
        self,
        peer_id: EndpointId,
        connection: iroh.Connection,
        *,
        outgoing: bool,
    ) -> None:
        loop = asyncio.get_running_loop()
        end = loop.time() + _PATH_WATCH_DURATION
        while loop.time() < end:
            await asyncio.sleep(_PATH_WATCH_INTERVAL)
            if connection.close_reason() is not None:
                return
            self._report_path(peer_id, connection, outgoing=outgoing)

    def _is_allowed(self, peer_id: EndpointId) -> bool:
        for removed in self._allowlist.reload():
            logger.warning(
                '%s: closing connections to peer %s which was removed from '
                'the allowlist',
                self._log_prefix(),
                removed.log_name('removed'),
            )
            self._close_peer(removed, CloseCode.NOT_ALLOWED, b'not allowed')
        return self._allowlist.allowed(peer_id)

    def _close_peer(
        self,
        peer_id: EndpointId,
        code: CloseCode,
        reason: bytes,
    ) -> None:
        outgoing = self._outgoing.pop(peer_id, None)
        incoming = self._incoming.pop(peer_id, set())
        for connection in (outgoing, *incoming):
            if connection is not None:
                connection.close(code, reason)

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _wait_online(self, timeout: float) -> None:
        try:
            await asyncio.wait_for(self.endpoint.online(), timeout=timeout)
        except TimeoutError:
            logger.warning(
                '%s: not connected to a home relay after %s seconds. '
                'Peers may only be able to connect directly',
                self._log_prefix(),
                timeout,
            )
        else:
            logger.info('%s: connected to home relay', self._log_prefix())

    async def _accept_loop(self) -> None:
        while True:
            incoming = await self.endpoint.accept_next()
            if incoming is None:  # pragma: no cover
                # Endpoint was closed.
                return
            self._spawn(self._handle_incoming(incoming))

    async def _handle_incoming(self, incoming: iroh.Incoming) -> None:
        try:
            accepting = await incoming.accept()
            connection = await accepting.connect()
        except iroh.IrohError as e:
            logger.debug(
                '%s: failed to accept connection: %s',
                self._log_prefix(),
                _message(e),
            )
            return

        peer_id = EndpointId.from_str(str(connection.remote_id()))
        if not self._is_allowed(peer_id):
            logger.warning(
                '%s: refused connection from endpoint %s which is not in the '
                'allowlist',
                self._log_prefix(),
                peer_id,
            )
            connection.close(CloseCode.NOT_ALLOWED, b'not allowed')
            return

        logger.info(
            '%s: accepted connection from peer %s',
            self._log_prefix(),
            self._peer_name(peer_id),
        )
        connections = self._incoming.setdefault(peer_id, set())
        connections.add(connection)
        await self._remember_addr(peer_id)
        self._watch_path(peer_id, connection, outgoing=False)
        try:
            while True:
                try:
                    stream = await connection.accept_bi()
                except iroh.IrohError:
                    # Connection was closed.
                    break
                if not self._is_allowed(peer_id):
                    connection.close(CloseCode.NOT_ALLOWED, b'not allowed')
                    break
                self._spawn(self._handle_stream(peer_id, stream))
        finally:
            connections.discard(connection)
            self._paths.pop(connection.stable_id(), None)
            if len(connections) == 0 and (
                self._incoming.get(peer_id) is connections
            ):
                del self._incoming[peer_id]
            logger.info(
                '%s: connection from peer %s closed',
                self._log_prefix(),
                self._peer_name(peer_id),
            )

    async def _handle_stream(
        self,
        peer_id: EndpointId,
        stream: iroh.BiStream,
    ) -> None:
        assert self._handler is not None
        response: tuple[int, dict[str, Any] | None, BytesLike | None]
        try:
            try:
                header, meta, data = await _read_message(
                    stream.recv(),
                    max_data_size=self._max_request_size,
                )
            except _DataTooLargeError as e:
                await stream.recv().stop(_STOP_REJECTED)
                response = (Status.TOO_LARGE, {'error': str(e)}, None)
            except EndpointProtocolError as e:
                await stream.recv().stop(_STOP_REJECTED)
                response = (Status.BAD_REQUEST, {'error': str(e)}, None)
            else:
                try:
                    response = await self._handler(
                        peer_id,
                        header.code,
                        meta,
                        data,
                    )
                except Exception as e:
                    logger.exception(
                        '%s: unexpected error handling request from %s',
                        self._log_prefix(),
                        self._peer_name(peer_id),
                    )
                    error = {'error': f'unexpected error: {e!r}'}
                    response = (Status.ERROR, error, None)
            await _write_message(stream.send(), *response)
        except iroh.IrohError as e:
            logger.debug(
                '%s: stream from %s failed: %s',
                self._log_prefix(),
                self._peer_name(peer_id),
                _message(e),
            )


async def _write_message(
    stream: iroh.SendStream,
    code: int,
    meta: dict[str, Any] | None,
    data: BytesLike | None,
) -> None:
    view = memoryview(b'' if data is None else data).cast('B')
    await stream.write_all(pack_message(code, meta, len(view)))
    for start in range(0, len(view), _CHUNK_SIZE):
        chunk = view[start : start + _CHUNK_SIZE]
        await stream.write_all(
            data
            if len(chunk) == len(view) and isinstance(data, bytes)
            # The bindings only accept bytes.
            else chunk.tobytes(),
        )
    await stream.finish()


async def _read_message(
    stream: iroh.RecvStream,
    *,
    max_data_size: int | None,
) -> tuple[Header, dict[str, Any], bytes | bytearray]:
    header = Header.unpack(await stream.read_exact(Header.SIZE))
    meta = (
        decode_meta(await stream.read_exact(header.meta_len))
        if header.meta_len > 0
        else {}
    )
    if max_data_size is not None and header.data_len > max_data_size:
        raise _DataTooLargeError(
            f'Data size ({header.data_len} bytes) exceeds the maximum object '
            f'size of the endpoint ({max_data_size} bytes).',
        )
    if header.data_len == 0:
        return header, meta, b''
    if header.data_len <= _CHUNK_SIZE:
        return header, meta, await stream.read_exact(header.data_len)
    data = bytearray(header.data_len)
    view = memoryview(data)
    for start in range(0, header.data_len, _CHUNK_SIZE):
        size = min(_CHUNK_SIZE, header.data_len - start)
        view[start : start + size] = await stream.read_exact(size)
    return header, meta, data


def _closed_with(reason: str, code: CloseCode) -> bool:
    # The bindings only expose the reason a connection was closed as a string
    # (e.g., "closed by peer: not allowed (code 1)").
    return reason.endswith(f'(code {int(code)})')


def _message(error: iroh.IrohError) -> str:
    return error.message()
