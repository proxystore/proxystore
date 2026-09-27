"""Endpoint serving.

The [`EndpointService`][proxystore.endpoint.serve.EndpointService] runs an
endpoint from its directory, and [`serve()`][proxystore.endpoint.serve.serve]
runs the service in the current process until it receives a signal.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import ssl
from types import TracebackType
from typing import Self

import uvloop

from proxystore.endpoint.auth import EndpointToken
from proxystore.endpoint.auth import TLSCertificate
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import resolve_host
from proxystore.endpoint.directory import ConnectionInfo
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.dispatch import Dispatcher
from proxystore.endpoint.p2p.manager import PeerManager
from proxystore.endpoint.server import ClientHandler
from proxystore.endpoint.storage import DictStorage
from proxystore.endpoint.storage import SQLiteStorage
from proxystore.endpoint.storage import Storage
from proxystore.utils.environment import hostname

logger = logging.getLogger(__name__)


def _create_peer_manager(
    endpoint_dir: EndpointDir,
    config: EndpointConfig,
) -> PeerManager | None:
    if not config.p2p.enabled:
        return None

    relays = config.p2p.relays
    logger.info(
        'Loaded %d peer(s) from %s, using relays: %s',
        len(endpoint_dir.peers.read().peers),
        endpoint_dir.peers_path,
        relays if isinstance(relays, str) else ', '.join(relays),
    )
    return PeerManager.from_endpoint_dir(endpoint_dir)


def _create_storage(
    endpoint_dir: EndpointDir,
    config: EndpointConfig,
) -> Storage:
    database_path = config.storage.database_path
    if database_path is not None:
        if database_path != ':memory:':
            database_path = endpoint_dir.resolve_path(database_path)
        logger.info(
            'Using SQLite database for storage (path: %s)',
            database_path,
        )
        return SQLiteStorage(
            database_path,
            max_object_size=config.storage.object_size_limit,
        )
    logger.warning('Database path not provided. Data will not be persisted')
    return DictStorage(max_object_size=config.storage.object_size_limit)


async def _close_server(
    server: asyncio.Server,
    handler: ClientHandler,
) -> None:
    server.close()
    await handler.close_connections()
    await server.wait_closed()


class EndpointService:
    """Service which serves an endpoint to clients.

    The service owns everything needed to run an endpoint from its
    directory: the storage and peer manager of the endpoint, the dispatcher
    which handles requests, the server that accepts client connections, and
    the connection file that clients use to connect. Once started, the
    endpoint is accepting client connections and its connection file is in
    the endpoint directory. When stopped, the connection file is removed,
    client connections are closed, and the storage is closed.

    Example:
        ```python
        async with EndpointService(endpoint_dir) as service:
            client = EndpointClient.from_dir(endpoint_dir)
            ...
        ```

    Args:
        endpoint_dir: Directory of the endpoint with its configuration.
    """

    def __init__(self, endpoint_dir: EndpointDir) -> None:
        self.endpoint_dir = endpoint_dir
        self._stack: contextlib.AsyncExitStack | None = None
        self._config: EndpointConfig | None = None
        self._dispatcher: Dispatcher | None = None
        self._connection: ConnectionInfo | None = None

    @property
    def running(self) -> bool:
        """The service has been started and not stopped."""
        return self._stack is not None

    @property
    def config(self) -> EndpointConfig:
        """Configuration of the running endpoint."""
        self._check_running()
        assert self._config is not None
        return self._config

    @property
    def dispatcher(self) -> Dispatcher:
        """Dispatcher which handles requests to the running endpoint."""
        self._check_running()
        assert self._dispatcher is not None
        return self._dispatcher

    @property
    def connection(self) -> ConnectionInfo:
        """Information clients use to connect to the endpoint."""
        self._check_running()
        assert self._connection is not None
        return self._connection

    def _check_running(self) -> None:
        if not self.running:
            raise RuntimeError('The endpoint service is not running.')

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
        """Start serving the endpoint.

        Raises:
            RuntimeError: If the service is already running.
            EndpointNotFoundError: If the configuration does not exist.
            EndpointConfigError: If the configuration is invalid or the ID in
                the configuration does not match the secret key.
            OSError: If the endpoint cannot listen on its host and port.
        """
        if self.running:
            raise RuntimeError('The endpoint service is already running.')
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
        """Stop serving the endpoint.

        This is idempotent so it is safe to call multiple times.
        """
        if self._stack is None:
            return
        logger.info('Shutting down endpoint server')
        stack, self._stack = self._stack, None
        await stack.aclose()

    async def _start(self, stack: contextlib.AsyncExitStack) -> None:
        endpoint_dir = self.endpoint_dir
        config = endpoint_dir.read_config()
        # The resolved host is only written to the connection file; the
        # configuration is never modified by a running endpoint.
        host = resolve_host(config.host)
        # Fail before starting if the secret key is missing or does not
        # match the configuration.
        endpoint_dir.read_secret_key()

        storage = _create_storage(endpoint_dir, config)
        stack.push_async_callback(storage.close)
        peer_manager = _create_peer_manager(endpoint_dir, config)
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
            max_object_size=config.storage.object_size_limit,
        )
        server = await handler.start_server(
            host,
            config.port,
            ssl_context=ssl_context,
        )
        stack.push_async_callback(_close_server, server, handler)

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
        logger.info('Config: %s', config)


async def _serve_async(endpoint_dir: EndpointDir) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    signals = (signal.SIGINT, signal.SIGTERM)
    # Signal handlers are installed before starting the endpoint so that a
    # signal received during start up stops the endpoint once it starts.
    for sig in signals:
        loop.add_signal_handler(sig, stop.set)
    try:
        async with EndpointService(endpoint_dir):
            await stop.wait()
    finally:
        for sig in signals:
            loop.remove_signal_handler(sig)


def serve(
    endpoint_dir: EndpointDir,
    *,
    log_level: int | str = logging.INFO,
    log_file: str | None = None,
    use_uvloop: bool = True,
) -> None:
    """Initialize and serve an endpoint.

    Warning:
        This function does not return until the server receives SIGINT or
        SIGTERM.

    Args:
        endpoint_dir: Directory of the endpoint with its configuration. The
            connection file is written to this directory while the
            endpoint is running.
        log_level: Logging level of endpoint.
        log_file: Optional file path to append log to.
        use_uvloop: Use uvloop as the event loop implementation.
    """
    if log_file is not None:
        parent_dir = os.path.dirname(log_file)
        if not os.path.isdir(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)
        logging.getLogger().handlers.append(logging.FileHandler(log_file))

    for handler in logging.getLogger().handlers:
        handler.setFormatter(
            logging.Formatter(
                '[%(asctime)s.%(msecs)03d] %(levelname)-5s (%(name)s) :: '
                '%(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
            ),
        )
    logging.getLogger().setLevel(log_level)

    # The remaining set up and serving code is deferred to within the
    # _serve_async helper function which will be executed within an event loop.
    try:
        if use_uvloop:  # pragma: no cover
            logger.info('Using uvloop as the event loop')
            uvloop.run(_serve_async(endpoint_dir))
        else:
            asyncio.run(_serve_async(endpoint_dir))
    except Exception as e:
        # Intercept exception so we can log it in the case that the endpoint
        # is running as a daemon process. Otherwise the user will never see
        # the exception.
        logger.exception('Caught unhandled exception: %r', e)
        raise
    except KeyboardInterrupt:  # pragma: no cover
        # SIGINT is handled by _serve_async once the event loop is running,
        # but can still be raised before then.
        pass
    finally:
        logger.info('Finished serving endpoint in %s', endpoint_dir)
