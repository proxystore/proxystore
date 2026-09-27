"""Manager of peer connections to other endpoints.

Warning:
    This module is an internal implementation detail. Its interface may
    change between releases without notice (see
    [`proxystore.endpoint`][proxystore.endpoint]).
"""

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
from typing import Protocol
from typing import Self

import iroh

from proxystore.endpoint.config import EndpointP2PConfig
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.exceptions import ObjectSizeExceededError
from proxystore.endpoint.exceptions import PeerConnectionTimeoutError
from proxystore.endpoint.exceptions import PeerNotAllowedError
from proxystore.endpoint.exceptions import PeerUnavailableError
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.identity import SecretKey
from proxystore.endpoint.p2p.addrs import PeerAddrCache
from proxystore.endpoint.protocol import ALPN
from proxystore.endpoint.protocol import Message
from proxystore.endpoint.protocol import MessageReader
from proxystore.endpoint.protocol import Status
from proxystore.utils.tasks import spawn_guarded_background_task

logger = logging.getLogger(__name__)

_CHUNK_SIZE = 64 * 1024 * 1024
# The iroh bindings limit the size of a single read to a u32, and each write
# copies the data, so data is read and written in chunks.

_STOP_REJECTED = 1
# Error code used to stop reading a request which is rejected.

RequestHandler = Callable[[EndpointId, Message], Awaitable[Message]]
"""Handler of requests from peers.

The handler is called with the ID of the peer and the request and returns
the response.
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


class PeerPolicy(Protocol):
    """Policy of which peer endpoints an endpoint communicates with.

    The [`PeerManager`][proxystore.endpoint.p2p.manager.PeerManager] checks
    the policy on each connection and request. The
    [`Allowlist`][proxystore.endpoint.peers.Allowlist] is the policy of
    endpoints started from an endpoint directory.
    """

    def allowed(self, peer_id: EndpointId) -> bool:
        """Check if the endpoint is allowed to communicate with this one."""
        ...

    def name_of(self, peer_id: EndpointId) -> str | None:
        """Get the name of the peer used in logs or `None` if unknown."""
        ...

    def revoked(self) -> set[EndpointId]:
        """Get the peers which are no longer allowed.

        The peer manager closes the connections to these peers.

        Returns:
            The peers which were allowed when this method was last called \
            but are no longer allowed.
        """
        ...


@dataclasses.dataclass(frozen=True)
class PeerOptions:
    """Options of the connections of a peer manager to peers.

    Attributes:
        preset: iroh preset used to configure discovery and relays. `None`
            uses `iroh.preset_n0()` which uses n0's public relays and DNS
            discovery.
        relay_mode: Relay mode which overrides the relays of the preset or
            `None` to use the relays of the preset.
        bind_addr: Address to bind to (e.g., `"127.0.0.1:0"`) or `None` to
            bind to all interfaces on a random port.
        connect_timeout: Timeout in seconds when connecting to a peer.
        online_timeout: Timeout in seconds to wait for the endpoint to
            connect to its home relay before logging a warning. If `None`,
            the endpoint does not wait (e.g., because relays are disabled).
    """

    preset: iroh.Preset | None = None
    relay_mode: iroh.RelayMode | None = None
    bind_addr: str | None = None
    connect_timeout: float = 30
    online_timeout: float | None = 10

    @classmethod
    def from_config(cls, config: EndpointP2PConfig) -> Self:
        """Get the options for a peer-to-peer configuration.

        The preset determines the discovery service, and the relay mode
        overrides the relays of the preset.
        """
        # The n0 preset uses n0's relays and discovery. The minimal preset
        # uses neither.
        if config.discovery == 'n0':
            preset = iroh.preset_n0()
            n0_relays = None
        else:
            preset = iroh.preset_minimal()
            n0_relays = iroh.RelayMode.default_mode()

        if config.relays == 'n0':
            return cls(preset=preset, relay_mode=n0_relays)
        if config.relays == 'none':
            # Without relays, there is no home relay to wait on.
            return cls(
                preset=preset,
                relay_mode=iroh.RelayMode.disabled(),
                online_timeout=None,
            )
        return cls(
            preset=preset,
            relay_mode=iroh.RelayMode.custom_from_urls(config.relays),
        )


class CloseCode(enum.IntEnum):
    """Application error codes used when closing a peer connection."""

    SHUTDOWN = 0
    """Endpoint is shutting down."""
    NOT_ALLOWED = 1
    """Peer is not in the allowlist of the endpoint."""


class PeerManager:
    """Manager of connections to peer endpoints.

    The manager binds an iroh endpoint using the secret key of the ProxyStore
    endpoint, accepts connections from peers, and sends requests to peers.
    Each request is sent on its own bidirectional stream. A connection to a
    peer is used in both directions: requests to the peer are sent on the
    most recent connection, whichever endpoint opened it, and requests from
    the peer are accepted on every connection. Two connections to a peer
    only exist if both endpoints connect to each other at the same time.

    The manager only communicates with peers allowed by its
    [`PeerPolicy`][proxystore.endpoint.p2p.manager.PeerPolicy]. Connections
    from other endpoints are refused, and requests to other endpoints fail
    with
    [`PeerNotAllowedError`][proxystore.endpoint.exceptions.PeerNotAllowedError].
    The policy is checked on each connection and request, and connections to
    peers which are no longer allowed are closed.

    Example:
        ```python
        manager = PeerManager(secret_key, Allowlist(endpoint_dir.peers_path))
        await manager.start(handler)
        response = await manager.request(peer_id, Message(Op.GET, meta))
        await manager.close()
        ```

    Args:
        secret_key: Secret key of the endpoint.
        policy: Policy of which peers are allowed.
        options: Options of connections to peers. Defaults to
            [`PeerOptions()`][proxystore.endpoint.p2p.manager.PeerOptions].
        max_request_size: Maximum size in bytes of the data in a request from
            a peer or `None` for no limit.
        addr_cache: Optional cache where the addresses of peers are saved
            (see
            [`PeerAddrCache`][proxystore.endpoint.p2p.addrs.PeerAddrCache]).
            Cached addresses are used when connecting to peers so peers can
            be reached even if discovery is unavailable.
    """

    def __init__(
        self,
        secret_key: SecretKey,
        policy: PeerPolicy,
        *,
        options: PeerOptions | None = None,
        max_request_size: int | None = None,
        addr_cache: PeerAddrCache | None = None,
    ) -> None:
        self._secret_key = secret_key
        self._id = secret_key.endpoint_id
        self._policy = policy
        self._options = PeerOptions() if options is None else options
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
        # Open connections to each peer. Requests from the peer are
        # accepted on all of them.
        self._connections: dict[EndpointId, set[iroh.Connection]] = {}
        # Connection used to send requests to each peer.
        self._preferred: dict[EndpointId, iroh.Connection] = {}
        # Last reported path of each connection by stable ID.
        self._paths: dict[int, tuple[bool, str] | None] = {}
        # If each connection was dialed by this endpoint by stable ID.
        self._directions: dict[int, bool] = {}
        self._closed = False

    @property
    def id(self) -> EndpointId:
        """ID of this endpoint."""
        return self._id

    @property
    def policy(self) -> PeerPolicy:
        """Policy of which peers are allowed."""
        return self._policy

    @property
    def options(self) -> PeerOptions:
        """Options of connections to peers."""
        return self._options

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
            The path of the connection used to send requests to the peer or \
            `None` if there is no open connection.
        """
        connection = self._preferred.get(peer_id)
        if connection is None or connection.close_reason() is not None:
            return None
        return PathInfo.from_connection(connection)

    def addr(self) -> iroh.EndpointAddr:
        """Get the current address of this endpoint.

        The address contains the ID, home relay URL, and direct addresses
        of the endpoint. Other endpoints can use the address to connect to
        this endpoint without discovery (see
        [`add_peer_addr()`][proxystore.endpoint.p2p.manager.PeerManager.add_peer_addr]).
        """
        return self.endpoint.addr()

    def add_peer_addr(self, addr: iroh.EndpointAddr) -> None:
        """Add a known address of a peer to use when connecting to the peer.

        This is useful when discovery is unavailable.
        """
        peer_id = EndpointId.from_str(str(addr.id()))
        self._addr_hints[peer_id] = addr

    def peer_name(self, peer_id: EndpointId) -> str:
        """Format the ID of a peer with its name for logging.

        The name is from the peer policy or `unknown` if the peer has no
        name (see
        [`EndpointId.log_name()`][proxystore.endpoint.identity.EndpointId.log_name]).
        """
        name = self._policy.name_of(peer_id)
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
                'Loaded %d cached peer address(es) from %s',
                len(cached),
                self._addr_cache.path,
            )
        # uniffi_set_event_loop() is intentionally not called. It sets a
        # process-wide event loop that the bindings then use for every call,
        # which breaks when a different event loop is used later. It is only
        # needed for callbacks from Rust into Python which are not used.
        options = self._options
        self._endpoint = await iroh.Endpoint.bind(
            iroh.EndpointOptions(
                preset=(
                    iroh.preset_n0()
                    if options.preset is None
                    else options.preset
                ),
                secret_key=self._secret_key.to_bytes(),
                alpns=[ALPN],
                relay_mode=options.relay_mode,
                bind_addr=options.bind_addr,
            ),
        )
        self._accept_task = spawn_guarded_background_task(self._accept_loop)
        self._accept_task.set_name(f'peer-manager-{self.id}-accept')
        if options.online_timeout is not None:
            self._online_task = asyncio.create_task(
                self._wait_online(options.online_timeout),
            )
        logger.info(
            'Listening for peer connections on %s',
            ', '.join(self.endpoint.bound_sockets()),
        )

    async def close(self) -> None:
        """Close all peer connections and the endpoint.

        This is idempotent so it is safe to call multiple times.
        """
        if self._closed:
            return
        self._closed = True
        # Connections are closed before the tasks are cancelled because
        # cancelling the task serving a connection forgets the connection
        # without closing it.
        for peer_id in list(self._connections):
            self._close_peer(peer_id, CloseCode.SHUTDOWN, b'shutdown')
        for task in (self._accept_task, self._online_task, *self._tasks):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        if self._endpoint is not None:
            await self._endpoint.close()
        logger.info('Peer manager closed')

    async def request(
        self,
        peer_id: EndpointId,
        request: Message,
    ) -> Message:
        """Send a request to a peer and wait for the response.

        Args:
            peer_id: ID of the peer.
            request: Request message.

        Returns:
            The response message.

        Raises:
            PeerNotAllowedError: If the peer is not in the allowlist or the
                peer refused the connection.
            PeerConnectionTimeoutError: If connecting to the peer times out.
            PeerUnavailableError: If the request fails.
        """
        if not self._is_allowed(peer_id):
            raise PeerNotAllowedError(
                f'Endpoint {peer_id} is not in the allowlist of peers. Add '
                'the endpoint with "proxystore-endpoint peers add".',
            )

        for attempt in range(2):
            connection, fresh = await self._get_connection(peer_id)
            try:
                response = await self._exchange(connection, request)
                self._report_path(peer_id, connection)
                return response
            except iroh.IrohError as e:
                reason = connection.close_reason()
                self._drop_preferred(peer_id, connection)
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
                    raise PeerUnavailableError(
                        f'Request to peer {peer_id} failed: {_message(e)}',
                    ) from None
                logger.debug(
                    'Retrying request to %s with a new connection: %s',
                    self.peer_name(peer_id),
                    _message(e),
                )
        raise AssertionError('Unreachable.')

    async def _exchange(
        self,
        connection: iroh.Connection,
        request: Message,
    ) -> Message:
        stream = await connection.open_bi()
        try:
            await _write_message(stream.send(), request)
        except iroh.IrohError as e:
            # The peer stops reading a request it rejects (e.g., because the
            # data is too large) but still sends a response with the reason.
            write_error: iroh.IrohError | None = e
        else:
            write_error = None
        try:
            return await _read_message(stream.recv(), MessageReader())
        except iroh.IrohError:
            if write_error is not None:
                raise write_error from None
            raise

    async def _get_connection(
        self,
        peer_id: EndpointId,
    ) -> tuple[iroh.Connection, bool]:
        async with self._dial_locks[peer_id]:
            connection = self._preferred.get(peer_id)
            if connection is not None and connection.close_reason() is None:
                return connection, False

            logger.info(
                'Connecting to peer %s',
                self.peer_name(peer_id),
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
            except PeerUnavailableError as e:
                if hint is None:
                    raise
                # The cached address may be stale so try again using only
                # discovery.
                logger.info(
                    'Failed to connect to peer %s using its cached '
                    'address, retrying with discovery: %s',
                    self.peer_name(peer_id),
                    e,
                )
                self._addr_hints.pop(peer_id, None)
                connection = await self._dial(peer_id, id_only)
            logger.info(
                'Connected to peer %s',
                self.peer_name(peer_id),
            )
            await self._add_connection(peer_id, connection, dialed=True)
            return connection, True

    async def _add_connection(
        self,
        peer_id: EndpointId,
        connection: iroh.Connection,
        *,
        dialed: bool,
    ) -> None:
        # The newest connection is used to send requests because an older
        # connection is likely to be closed (e.g., because the peer
        # restarted). If both peers connected to each other at the same
        # time, each peer may prefer a different connection which is fine
        # because requests are accepted on all connections.
        self._connections.setdefault(peer_id, set()).add(connection)
        self._preferred[peer_id] = connection
        self._directions[connection.stable_id()] = dialed
        await self._remember_addr(peer_id)
        self._watch_path(peer_id, connection)
        self._spawn(self._serve_connection(peer_id, connection))

    async def _dial(
        self,
        peer_id: EndpointId,
        addr: iroh.EndpointAddr,
    ) -> iroh.Connection:
        try:
            return await asyncio.wait_for(
                self.endpoint.connect(addr, ALPN),
                timeout=self._options.connect_timeout,
            )
        except TimeoutError:
            raise PeerConnectionTimeoutError(
                f'Connecting to peer {peer_id} timed out after '
                f'{self._options.connect_timeout} seconds.',
            ) from None
        except iroh.IrohError as e:
            raise PeerUnavailableError(
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
                if self._policy.allowed(peer)
            }
            try:
                self._addr_cache.save(addrs)
            except OSError as e:
                logger.warning(
                    'Failed to save peer address cache: %s',
                    e,
                )

    def _drop_preferred(
        self,
        peer_id: EndpointId,
        connection: iroh.Connection,
    ) -> None:
        if self._preferred.get(peer_id) is connection:
            del self._preferred[peer_id]

    def _report_path(
        self,
        peer_id: EndpointId,
        connection: iroh.Connection,
    ) -> None:
        """Log the path of a connection if it changed."""
        dialed = self._directions.get(connection.stable_id(), True)
        direction = 'to' if dialed else 'from'
        path = PathInfo.from_connection(connection)
        key = None if path is None else (path.relayed, path.remote_addr)
        stable_id = connection.stable_id()
        if stable_id in self._paths and self._paths[stable_id] == key:
            return
        self._paths[stable_id] = key
        if path is None:
            logger.info(
                'Connection %s peer %s has no path',
                direction,
                self.peer_name(peer_id),
            )
        else:
            logger.info(
                'Connection %s peer %s is %s',
                direction,
                self.peer_name(peer_id),
                path.describe(),
            )

    def _watch_path(
        self,
        peer_id: EndpointId,
        connection: iroh.Connection,
    ) -> None:
        self._report_path(peer_id, connection)
        self._spawn(self._watch_path_changes(peer_id, connection))

    async def _watch_path_changes(
        self,
        peer_id: EndpointId,
        connection: iroh.Connection,
    ) -> None:
        loop = asyncio.get_running_loop()
        end = loop.time() + _PATH_WATCH_DURATION
        while loop.time() < end:
            await asyncio.sleep(_PATH_WATCH_INTERVAL)
            if connection.close_reason() is not None:
                return
            self._report_path(peer_id, connection)

    def _is_allowed(self, peer_id: EndpointId) -> bool:
        for removed in self._policy.revoked():
            logger.warning(
                'Closing connections to peer %s which was removed from '
                'the allowlist',
                removed.log_name('removed'),
            )
            self._close_peer(removed, CloseCode.NOT_ALLOWED, b'not allowed')
        return self._policy.allowed(peer_id)

    def _close_peer(
        self,
        peer_id: EndpointId,
        code: CloseCode,
        reason: bytes,
    ) -> None:
        self._preferred.pop(peer_id, None)
        for connection in self._connections.pop(peer_id, set()):
            connection.close(code, reason)

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> None:
        # Tasks are named after their coroutine (e.g.,
        # PeerManager._handle_stream) for the logs.
        task = asyncio.create_task(
            coro, name=getattr(coro, '__qualname__', None)
        )
        self._tasks.add(task)
        task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task[None]) -> None:
        # Unexpected errors in tasks for a peer are logged when the task
        # finishes rather than when it is garbage collected. The endpoint
        # keeps running because one peer should not stop the endpoint.
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.error(
                'Unexpected error in task %s',
                task.get_name(),
                exc_info=task.exception(),
            )

    async def _wait_online(self, timeout: float) -> None:
        # Errors are logged rather than raised because this task only
        # reports the status of the relay and close() awaits it.
        try:
            await asyncio.wait_for(self.endpoint.online(), timeout=timeout)
        except TimeoutError:
            logger.warning(
                'Not connected to a home relay after %s seconds. '
                'Peers may only be able to connect directly',
                timeout,
            )
        except Exception:
            logger.exception('Failed to wait for a home relay connection')
        else:
            logger.info('Connected to home relay')

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
                'Failed to accept connection: %s',
                _message(e),
            )
            return

        peer_id = EndpointId.from_str(str(connection.remote_id()))
        if not self._is_allowed(peer_id):
            logger.warning(
                'Refused connection from endpoint %s which is not in the '
                'allowlist',
                peer_id,
            )
            connection.close(CloseCode.NOT_ALLOWED, b'not allowed')
            return

        logger.info(
            'Accepted connection from peer %s',
            self.peer_name(peer_id),
        )
        await self._add_connection(peer_id, connection, dialed=False)

    async def _serve_connection(
        self,
        peer_id: EndpointId,
        connection: iroh.Connection,
    ) -> None:
        """Accept requests from the peer on a connection until it closes."""
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
            connections = self._connections.get(peer_id, set())
            connections.discard(connection)
            if len(connections) == 0:
                self._connections.pop(peer_id, None)
            self._drop_preferred(peer_id, connection)
            self._paths.pop(connection.stable_id(), None)
            self._directions.pop(connection.stable_id(), None)
            logger.info(
                'Connection with peer %s closed',
                self.peer_name(peer_id),
            )

    async def _handle_stream(
        self,
        peer_id: EndpointId,
        stream: iroh.BiStream,
    ) -> None:
        assert self._handler is not None
        response: Message
        try:
            try:
                request = await _read_message(
                    stream.recv(),
                    MessageReader(max_data_size=self._max_request_size),
                )
            except (EndpointProtocolError, ObjectSizeExceededError) as e:
                await stream.recv().stop(_STOP_REJECTED)
                response = Message.from_error(e)
            else:
                try:
                    response = await self._handler(peer_id, request)
                except Exception as e:
                    logger.exception(
                        'Unexpected error handling request from %s',
                        self.peer_name(peer_id),
                    )
                    response = Message.error(
                        Status.ERROR,
                        f'unexpected error: {e!r}',
                    )
            await _write_message(stream.send(), response)
        except iroh.IrohError as e:
            logger.debug(
                'Stream from %s failed: %s',
                self.peer_name(peer_id),
                _message(e),
            )


async def _write_message(stream: iroh.SendStream, message: Message) -> None:
    data = message.data
    view = memoryview(data).cast('B')
    await stream.write_all(message.pack_head())
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
    reader: MessageReader,
) -> Message:
    while not reader.done:
        reader.feed(await _read_exact(stream, reader.size))
    return reader.message


async def _read_exact(stream: iroh.RecvStream, size: int) -> bytes | bytearray:
    if size <= _CHUNK_SIZE:
        return await stream.read_exact(size)
    data = bytearray(size)
    buffer = memoryview(data)
    for start in range(0, size, _CHUNK_SIZE):
        chunk = min(_CHUNK_SIZE, size - start)
        buffer[start : start + chunk] = await stream.read_exact(chunk)
    return data


def _closed_with(reason: str, code: CloseCode) -> bool:
    # The bindings only expose the reason a connection was closed as a string
    # (e.g., "closed by peer: not allowed (code 1)").
    return reason.endswith(f'(code {int(code)})')


def _message(error: iroh.IrohError) -> str:
    return error.message()
