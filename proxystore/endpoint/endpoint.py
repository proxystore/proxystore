"""ProxyStore endpoints.

Warning:
    This module is an internal implementation detail. Its interface may
    change between releases without notice (see
    [`proxystore.endpoint`][proxystore.endpoint]).

An [`Endpoint`][proxystore.endpoint.endpoint.Endpoint] runs an endpoint
from its directory in the current event loop. Use
[`serve()`][proxystore.endpoint.process.serve] to run an endpoint in the
current process until it receives a signal, or
[`start_endpoint()`][proxystore.endpoint.process.start_endpoint] to run an
endpoint as a daemon.
"""

from __future__ import annotations

import contextlib
import logging
import os
import ssl
from types import TracebackType
from typing import Self

from proxystore.endpoint.auth import EndpointToken
from proxystore.endpoint.auth import TLSCertificate
from proxystore.endpoint.config import DEFAULT_DATABASE_PATH
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import resolve_host
from proxystore.endpoint.directory import ConnectionInfo
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.dispatch import Dispatcher
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.identity import SecretKey
from proxystore.endpoint.p2p.addrs import PeerAddrCache
from proxystore.endpoint.p2p.manager import PeerManager
from proxystore.endpoint.p2p.manager import PeerOptions
from proxystore.endpoint.p2p.manager import PeerPolicy
from proxystore.endpoint.peers import Allowlist
from proxystore.endpoint.server import ClientHandler
from proxystore.endpoint.storage import MemoryStorage
from proxystore.endpoint.storage import SQLiteStorage
from proxystore.endpoint.storage import Storage
from proxystore.utils.environment import hostname

logger = logging.getLogger(__name__)


class Endpoint:
    """ProxyStore endpoint.

    An endpoint is an object store which serves clients on the local network
    and forwards requests from its clients to peer endpoints.

    The endpoint owns everything needed to run from its directory: the
    storage of objects, the
    [`PeerManager`][proxystore.endpoint.p2p.manager.PeerManager] which
    communicates with peers, the
    [`Dispatcher`][proxystore.endpoint.dispatch.Dispatcher] which handles
    requests, the server that accepts client connections, and the connection
    file that clients use to connect. The endpoint is the only component
    which reads the endpoint directory, and it reads the configuration and
    secret key once each time it starts.

    Once started, the endpoint holds the lock of its directory (see
    [`EndpointDir.lock()`][proxystore.endpoint.directory.EndpointDir.lock]),
    is accepting client connections, and its connection file is in the
    endpoint directory. When stopped, the connection file is removed, client
    connections are closed, the peer manager is closed, the storage is
    closed, and the lock is released. An endpoint can be started again after
    it is stopped.

    Example:
        ```python
        async with Endpoint(EndpointDir.from_name('my-endpoint')) as endpoint:
            client = EndpointClient.from_dir(endpoint.endpoint_dir)
            ...
        ```

    Args:
        endpoint_dir: Directory of the endpoint with its configuration.
        storage: Storage of the endpoint. Defaults to the storage backend
            in the configuration. The endpoint closes the storage when it
            stops.
        peer_policy: Policy of which peers are allowed. Defaults to the
            [`Allowlist`][proxystore.endpoint.peers.Allowlist] of the peers
            file in the endpoint directory. Only used if peering is enabled.
        peer_options: Options of connections to peers. Defaults to the
            options for the peer-to-peer configuration (see
            [`PeerOptions.from_config()`][proxystore.endpoint.p2p.manager.PeerOptions.from_config]).
            Only used if peering is enabled.
    """

    def __init__(
        self,
        endpoint_dir: EndpointDir,
        *,
        storage: Storage | None = None,
        peer_policy: PeerPolicy | None = None,
        peer_options: PeerOptions | None = None,
    ) -> None:
        self.endpoint_dir = endpoint_dir
        self._storage = storage
        self._peer_policy = peer_policy
        self._peer_options = peer_options

        self._stack: contextlib.AsyncExitStack | None = None
        self._config: EndpointConfig | None = None
        self._dispatcher: Dispatcher | None = None
        self._connection: ConnectionInfo | None = None

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self.endpoint_dir.path!r})'

    @property
    def running(self) -> bool:
        """The endpoint has been started and not stopped."""
        return self._stack is not None

    @property
    def config(self) -> EndpointConfig:
        """Configuration of the running endpoint."""
        self._check_running()
        assert self._config is not None
        return self._config

    @property
    def id(self) -> EndpointId:
        """ID of the running endpoint."""
        return self.config.id

    @property
    def name(self) -> str:
        """Name of the running endpoint."""
        return self.config.name

    @property
    def dispatcher(self) -> Dispatcher:
        """Dispatcher which handles requests to the running endpoint.

        The dispatcher is an implementation detail which is exposed for
        testing and benchmarking.
        """
        self._check_running()
        assert self._dispatcher is not None
        return self._dispatcher

    @property
    def peer_manager(self) -> PeerManager | None:
        """Peer manager of the running endpoint or `None` if peering is off.

        The peer manager is an implementation detail which is exposed for
        testing and benchmarking.
        """
        return self.dispatcher.peer_manager

    @property
    def connection(self) -> ConnectionInfo:
        """Information clients use to connect to the running endpoint."""
        self._check_running()
        assert self._connection is not None
        return self._connection

    def _check_running(self) -> None:
        if not self.running:
            raise RuntimeError('The endpoint is not running.')

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        await self.stop()

    async def start(self) -> None:
        """Start the endpoint.

        Raises:
            RuntimeError: If the endpoint is already running.
            EndpointRunningError: If another instance of the endpoint is
                running on this host or may be running on another host.
            EndpointNotFoundError: If the configuration does not exist.
            EndpointConfigError: If the configuration is invalid or the ID in
                the configuration does not match the secret key.
            OSError: If the endpoint cannot listen on its host and port.
        """
        if self.running:
            raise RuntimeError('The endpoint is already running.')
        # Resources are cleaned up in the reverse order they are created,
        # including when start up fails partway through.
        stack = contextlib.AsyncExitStack()
        try:
            await self._start(stack)
        except BaseException:
            await stack.aclose()
            raise
        self._stack = stack

    async def stop(self) -> None:
        """Stop the endpoint.

        This is idempotent so it is safe to call multiple times.
        """
        if self._stack is None:
            return
        logger.info('Shutting down endpoint')
        stack, self._stack = self._stack, None
        await stack.aclose()

    async def _start(self, stack: contextlib.AsyncExitStack) -> None:
        endpoint_dir = self.endpoint_dir
        config = endpoint_dir.read_config()
        endpoint_dir.check_stopped()
        # The lock is held for as long as the endpoint runs so the status of
        # the endpoint can be checked by other processes. Acquiring the lock
        # fails if another instance started since the status was checked.
        lock = endpoint_dir.lock()
        lock.acquire()
        stack.callback(lock.release)

        # The resolved host is only written to the connection file; the
        # configuration is never modified by a running endpoint.
        host = resolve_host(config.host)
        # Fail before starting if the secret key is missing or does not
        # match the configuration.
        secret_key = endpoint_dir.read_secret_key(config.id)

        storage = self._create_storage(config)
        stack.push_async_callback(storage.close)
        peer_manager = self._create_peer_manager(config, secret_key)
        dispatcher = Dispatcher(config.id, storage, peer_manager)
        if peer_manager is not None:
            # The peer manager is closed before the storage so no requests
            # from peers are handled after the storage is closed.
            await peer_manager.start(dispatcher.handle_peer_request)
            stack.push_async_callback(peer_manager.close)

        if endpoint_dir.restrict_permissions():
            logger.warning(
                'Removed group and other permissions from '
                '%s because clients trust the files in the '
                'endpoint directory and it contains the secret key',
                endpoint_dir,
            )

        token = EndpointToken.generate()
        ssl_context: ssl.SSLContext | None = None
        tls_fingerprint: str | None = None
        if config.tls:
            certificate = TLSCertificate.generate(
                f'proxystore-endpoint-{config.id.short()}',
            )
            ssl_context = certificate.ssl_context()
            tls_fingerprint = certificate.fingerprint
            logger.info('Encrypting client connections with TLS')

        handler = ClientHandler(
            dispatcher,
            token,
            name=config.name,
            max_object_size=config.object_size_limit,
        )
        await handler.start(host, config.port, ssl_context=ssl_context)
        stack.push_async_callback(handler.close)

        # The connection file is only written once the server is listening
        # so that a failed start (e.g., because another instance of the
        # endpoint is using the port) does not replace or remove the
        # connection file of the running instance.
        connection = ConnectionInfo(
            host=host,
            port=config.port,
            token=token,
            tls_fingerprint=tls_fingerprint,
            hostname=hostname(),
            pid=os.getpid(),
        )
        endpoint_dir.write_connection(connection)
        stack.callback(endpoint_dir.remove_connection, connection)

        self._config = config
        self._dispatcher = dispatcher
        self._connection = connection
        logger.info(
            'Serving endpoint %s (%s) on %s:%s',
            config.id,
            config.name,
            host,
            config.port,
        )
        logger.info('Config: %s', config.model_dump_json())

    def _create_storage(self, config: EndpointConfig) -> Storage:
        if self._storage is not None:
            logger.info('Using storage %r', self._storage)
            return self._storage
        if config.storage.backend == 'sqlite':
            database_path = self.endpoint_dir.resolve_path(
                config.storage.database_path or DEFAULT_DATABASE_PATH,
            )
            logger.info(
                'Using SQLite database for storage (path: %s)',
                database_path,
            )
            return SQLiteStorage(database_path)
        logger.info('Storing objects in memory. Objects are lost on shutdown')
        return MemoryStorage()

    def _create_peer_manager(
        self,
        config: EndpointConfig,
        secret_key: SecretKey,
    ) -> PeerManager | None:
        if not config.p2p.enabled:
            logger.info('Peering is disabled')
            return None

        policy = self._peer_policy
        if policy is None:
            allowlist = Allowlist(self.endpoint_dir.peers_path)
            logger.info(
                'Loaded %d peer(s) from %s',
                len(allowlist.peers.peers),
                allowlist.path,
            )
            policy = allowlist

        relays = config.p2p.relays
        logger.info(
            'Using relays: %s',
            relays if isinstance(relays, str) else ', '.join(relays),
        )
        options = self._peer_options
        if options is None:
            options = PeerOptions.from_config(config.p2p)
        return PeerManager(
            secret_key,
            policy,
            options=options,
            max_request_size=config.object_size_limit,
            addr_cache=PeerAddrCache(self.endpoint_dir.peer_addrs_path),
        )
