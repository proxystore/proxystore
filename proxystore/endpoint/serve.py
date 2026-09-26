"""Endpoint serving.

Endpoints serve client requests over TCP using the
[`ClientHandler`][proxystore.endpoint.server.ClientHandler].
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import ssl
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

try:
    import uvloop
except ImportError as e:  # pragma: no cover
    raise ImportError(
        f'{e}. To enable endpoint serving, install proxystore with '
        '"pip install proxystore[endpoints]".',
    ) from e

from proxystore.endpoint.auth import ConnectionInfo
from proxystore.endpoint.auth import generate_tls_certificate
from proxystore.endpoint.auth import generate_token
from proxystore.endpoint.auth import pem_certificate_fingerprint
from proxystore.endpoint.auth import server_ssl_context
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.identity import endpoint_id_from_secret_key
from proxystore.endpoint.identity import short_id
from proxystore.endpoint.peers import Allowlist
from proxystore.endpoint.server import ClientHandler
from proxystore.endpoint.storage import DictStorage
from proxystore.endpoint.storage import SQLiteStorage
from proxystore.endpoint.storage import Storage

if TYPE_CHECKING:
    from proxystore.p2p.manager import PeerManager

logger = logging.getLogger(__name__)


def _check_secret_key(
    endpoint_dir: EndpointDir,
    config: EndpointConfig,
) -> bytes:
    try:
        secret_key = endpoint_dir.read_secret_key()
    except FileNotFoundError:
        raise FileNotFoundError(
            f'Endpoint directory {endpoint_dir} does not contain a secret '
            'key. Remove the endpoint and configure it again with '
            '"proxystore-endpoint configure".',
        ) from None
    endpoint_id = endpoint_id_from_secret_key(secret_key)
    if endpoint_id != config.id:
        raise ValueError(
            f'The endpoint ID in the configuration ({config.id}) does not '
            f'match the secret key ({endpoint_id}) in {endpoint_dir}.',
        )
    return secret_key


def _create_peer_manager(
    endpoint_dir: EndpointDir,
    config: EndpointConfig,
    secret_key: bytes,
) -> PeerManager | None:
    if not config.p2p.enabled:
        return None

    from proxystore.p2p.manager import PeerManager

    allowlist = Allowlist(endpoint_dir.peers_path)
    peers = len(allowlist.peers.peers)
    logger.info('Loaded %d peer(s) from %s', peers, allowlist.path)
    return PeerManager(
        secret_key,
        allowlist,
        max_request_size=config.storage.object_size_limit,
    )


def _create_storage(config: EndpointConfig) -> Storage:
    database_path = config.storage.database_path
    if database_path is not None:
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


@contextlib.asynccontextmanager
async def running_endpoint(
    endpoint_dir: EndpointDir,
) -> AsyncIterator[Endpoint]:
    """Run an endpoint that serves clients until the context exits.

    Once the context is entered, the endpoint is accepting client connections
    and its connection file is in the endpoint directory. When the context
    exits, the connection file is removed, client connections are closed,
    and the endpoint is closed.

    Example:
        ```python
        async with running_endpoint(endpoint_dir):
            client = EndpointClient.from_dir(endpoint_dir)
            ...
        ```

    Args:
        endpoint_dir: Directory of the endpoint with its configuration.

    Yields:
        The running endpoint.

    Raises:
        FileNotFoundError: If the configuration or secret key does not exist.
        ValueError: If the configuration is invalid, the host is not set
            in the configuration, or the ID in the configuration does not
            match the secret key.
        OSError: If the endpoint cannot listen on its host and port.
    """
    config = endpoint_dir.read_config()
    if config.host is None:
        raise ValueError('EndpointConfig has NoneType as host.')
    secret_key = _check_secret_key(endpoint_dir, config)

    # Resources are cleaned up in the reverse order they are created,
    # including when start up fails partway through.
    async with contextlib.AsyncExitStack() as stack:
        endpoint = await stack.enter_async_context(
            Endpoint(
                name=config.name,
                endpoint_id=config.id,
                peer_manager=_create_peer_manager(
                    endpoint_dir,
                    config,
                    secret_key,
                ),
                storage=_create_storage(config),
            ),
        )

        if endpoint_dir.restrict_permissions():
            logger.warning(
                'Removed group and other permissions from '
                '%s because clients trust the files in the '
                'endpoint directory and it contains the secret key',
                endpoint_dir,
            )

        token = generate_token()
        ssl_context: ssl.SSLContext | None = None
        tls_fingerprint: str | None = None
        if config.tls:
            cert_pem, key_pem = generate_tls_certificate(
                f'proxystore-endpoint-{short_id(config.id)}',
            )
            ssl_context = server_ssl_context(cert_pem, key_pem)
            tls_fingerprint = pem_certificate_fingerprint(cert_pem)
            logger.info('Encrypting client connections with TLS')

        handler = ClientHandler(
            endpoint,
            token,
            max_object_size=config.storage.object_size_limit,
        )
        server = await handler.start_server(
            config.host,
            config.port,
            ssl_context=ssl_context,
        )
        stack.push_async_callback(_close_server, server, handler)

        # The connection file is only written once the server is listening
        # so that a failed start (e.g., because another instance of the
        # endpoint is using the port) does not replace or remove the
        # connection file of the running instance.
        connection = ConnectionInfo(
            host=config.host,
            port=config.port,
            token=token,
            tls_fingerprint=tls_fingerprint,
        )
        endpoint_dir.write_connection(connection)
        stack.callback(endpoint_dir.remove_connection, connection)
        logger.info(
            'Serving endpoint %s (%s) on %s:%s',
            endpoint.id,
            endpoint.name,
            config.host,
            config.port,
        )
        logger.info('Config: %s', config)
        try:
            yield endpoint
        finally:
            logger.info('Shutting down endpoint server')


async def _serve_async(endpoint_dir: EndpointDir) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    signals = (signal.SIGINT, signal.SIGTERM)
    # Signal handlers are installed before starting the endpoint so that a
    # signal received during start up stops the endpoint once it starts.
    for sig in signals:
        loop.add_signal_handler(sig, stop.set)
    try:
        async with running_endpoint(endpoint_dir):
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
