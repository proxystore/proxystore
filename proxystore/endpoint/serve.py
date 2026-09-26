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
import uuid
from collections.abc import AsyncIterator
from typing import Any
from typing import Literal

try:
    import uvloop
except ImportError as e:  # pragma: no cover
    raise ImportError(
        f'{e}. To enable endpoint serving, install proxystore with '
        '"pip install proxystore[endpoints]".',
    ) from e

from aiortc import RTCIceServer
from globus_sdk.token_storage import TokenValidationError

from proxystore.endpoint.auth import create_server_ssl_context
from proxystore.endpoint.auth import Credentials
from proxystore.endpoint.auth import restrict_directory
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.server import ClientHandler
from proxystore.endpoint.storage import DictStorage
from proxystore.endpoint.storage import SQLiteStorage
from proxystore.endpoint.storage import Storage
from proxystore.globus.app import get_globus_app
from proxystore.globus.scopes import get_relay_scopes_by_resource_server
from proxystore.p2p.manager import PeerManager
from proxystore.p2p.nat import check_nat_and_log
from proxystore.p2p.relay.client import RelayClient

logger = logging.getLogger(__name__)


def _get_auth_headers(
    method: Literal['globus'] | None,
    **kwargs: Any,
) -> dict[str, str]:
    if method is None:
        return {}
    elif method == 'globus':
        app = get_globus_app()
        scopes = get_relay_scopes_by_resource_server()
        assert len(scopes) == 1
        app.add_scope_requirements(scopes)
        logger.info('Initialized Globus app')
        try:
            authorizer = app.get_authorizer(*scopes.keys())
        except TokenValidationError:
            logger.exception(
                'Failed to find valid tokens for the specified relay '
                'resource server. Have you logged in yet? If not, login then '
                'try again.\n  $ proxystore-globus-auth login',
            )
            raise SystemExit(1) from None
        bearer = authorizer.get_authorization_header()
        assert bearer is not None
        return {'Authorization': bearer}
    else:
        raise AssertionError('Unreachable.')


def _create_storage(config: EndpointConfig) -> Storage:
    database_path = config.storage.database_path
    if database_path is not None:
        logger.info(
            f'Using SQLite database for storage (path: {database_path})',
        )
        return SQLiteStorage(
            database_path,
            max_object_size=config.storage.max_object_size,
        )
    logger.warning('Database path not provided. Data will not be persisted')
    return DictStorage(max_object_size=config.storage.max_object_size)


def _create_peer_manager(config: EndpointConfig) -> PeerManager | None:
    if config.relay.address is None:
        return None

    headers = _get_auth_headers(
        method=config.relay.auth.method,
        **config.relay.auth.kwargs,
    )
    relay_client = RelayClient(
        address=config.relay.address,
        client_name=config.name,
        client_uuid=uuid.UUID(config.uuid),
        extra_headers=headers,
        verify_certificate=config.relay.verify_certificate,
    )
    ice_servers = (
        None
        if config.relay.ice_servers is None
        else [
            RTCIceServer(
                urls=server.urls,
                username=server.username,
                credential=server.credential,
            )
            for server in config.relay.ice_servers
        ]
    )
    return PeerManager(
        relay_client,
        peer_channels=config.relay.peer_channels,
        ice_servers=ice_servers,
    )


async def _cancel(task: asyncio.Task[Any]) -> None:
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def _close_server(
    server: asyncio.Server,
    handler: ClientHandler,
) -> None:
    server.close()
    handler.close_connections()
    await server.wait_closed()


@contextlib.asynccontextmanager
async def running_endpoint(
    config: EndpointConfig,
    endpoint_dir: str,
) -> AsyncIterator[Endpoint]:
    """Run an endpoint that serves clients until the context exits.

    Once the context is entered, the endpoint is accepting client connections
    and its credentials are in the endpoint directory. When the context
    exits, client connections are closed, the credentials are removed, and
    the endpoint is closed.

    Example:
        ```python
        async with running_endpoint(config, endpoint_dir):
            client = EndpointClient.from_config(config, endpoint_dir)
            ...
        ```

    Args:
        config: Configuration of the endpoint.
        endpoint_dir: Directory of the endpoint.

    Yields:
        The running endpoint.

    Raises:
        ValueError: If the host is not set in the configuration.
        OSError: If the endpoint cannot listen on its host and port.
    """
    if config.host is None:
        raise ValueError('EndpointConfig has NoneType as host.')

    # Resources are cleaned up in the reverse order they are created,
    # including when start up fails partway through.
    async with contextlib.AsyncExitStack() as stack:
        peer_manager = _create_peer_manager(config)
        if peer_manager is not None:
            # The NAT check only produces diagnostic logs so it is run
            # concurrently rather than delaying the endpoint from serving
            # requests on networks where STUN is slow or blocked.
            nat_check = asyncio.create_task(check_nat_and_log())
            stack.push_async_callback(_cancel, nat_check)

        endpoint = await stack.enter_async_context(
            Endpoint(
                name=config.name,
                uuid=uuid.UUID(config.uuid),
                peer_manager=peer_manager,
                storage=_create_storage(config),
            ),
        )

        if restrict_directory(endpoint_dir):
            logger.warning(
                'Removed group and other write permissions from '
                f'{endpoint_dir} because clients trust the files in the '
                'endpoint directory',
            )
        stack.callback(Credentials.remove, endpoint_dir)
        credentials = Credentials.create(
            endpoint_dir,
            tls=config.tls,
            common_name=f'proxystore-endpoint-{config.uuid}',
        )
        handler = ClientHandler(
            endpoint,
            credentials.token,
            max_object_size=config.storage.max_object_size,
        )

        ssl_context: ssl.SSLContext | None = None
        if config.tls:
            ssl_context = create_server_ssl_context(endpoint_dir)
            logger.info('Encrypting client connections with TLS')

        server = await handler.start_server(
            config.host,
            config.port,
            ssl_context=ssl_context,
        )
        stack.push_async_callback(_close_server, server, handler)
        logger.info(
            f'Serving endpoint {endpoint.uuid} ({endpoint.name}) on '
            f'{config.host}:{config.port}',
        )
        logger.info(f'Config: {config}')
        try:
            yield endpoint
        finally:
            logger.info('Shutting down endpoint server')


async def _serve_async(config: EndpointConfig, endpoint_dir: str) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    signals = (signal.SIGINT, signal.SIGTERM)
    # Signal handlers are installed before starting the endpoint so that a
    # signal received during start up stops the endpoint once it starts.
    for sig in signals:
        loop.add_signal_handler(sig, stop.set)
    try:
        async with running_endpoint(config, endpoint_dir):
            await stop.wait()
    finally:
        for sig in signals:
            loop.remove_signal_handler(sig)


def serve(
    config: EndpointConfig,
    *,
    endpoint_dir: str,
    log_level: int | str = logging.INFO,
    log_file: str | None = None,
    use_uvloop: bool = True,
) -> None:
    """Initialize and serve an endpoint.

    Warning:
        This function does not return until the server receives SIGINT or
        SIGTERM.

    Args:
        config: Configuration object.
        endpoint_dir: Directory of the endpoint. The client token file is
            written to this directory while the endpoint is running.
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
            uvloop.run(_serve_async(config, endpoint_dir))
        else:
            asyncio.run(_serve_async(config, endpoint_dir))
    except Exception as e:
        # Intercept exception so we can log it in the case that the endpoint
        # is running as a daemon process. Otherwise the user will never see
        # the exception.
        logger.exception(f'Caught unhandled exception: {e!r}')
        raise
    except KeyboardInterrupt:  # pragma: no cover
        # SIGINT is handled by _serve_async once the event loop is running,
        # but can still be raised before then.
        pass
    finally:
        logger.info(f'Finished serving endpoint: {config.name}')
