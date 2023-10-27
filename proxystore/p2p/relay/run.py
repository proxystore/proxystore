"""CLI and serving functions for running a Globus Auth relay server."""
from __future__ import annotations

import asyncio
import datetime
import logging
import logging.handlers
import os
import pprint
import signal
import ssl
import sys
from typing import TypeVar

import click
import websockets

from proxystore.p2p.relay.authenticate import get_authenticator
from proxystore.p2p.relay.config import RelayServingConfig
from proxystore.p2p.relay.server import RelayServer
from proxystore.utils.tasks import spawn_guarded_background_task

logger = logging.getLogger(__name__)
UserT = TypeVar('UserT')


def periodic_client_logger(
    server: RelayServer[UserT],
    interval: float = 60,
    limit: float | None = 60,
    level: int = logging.INFO,
) -> asyncio.Task[None]:
    """Create an asyncio task which logs currently connected clients.

    Args:
        server: Relay server instance to log connected clients of.
        interval: Seconds between logging connected clients.
        limit: Only log detailed client list if the number of clients is
            less than this number. Useful for debugging or avoiding
            clobbering the logs by printing thousands of clients.
        level: Logging level.

    Returns:
        Asyncio task.
    """

    async def _log() -> None:
        while True:
            await asyncio.sleep(interval)
            clients = server.client_manager.get_clients()
            clients = sorted(clients, key=lambda client: client.name)
            clients_repr = (
                '\n'.join(repr(client) for client in clients)
                if limit is not None
                else None
            )
            message = f'Connected clients: {len(clients)}'
            message = (
                f'{message}\n{clients_repr}'
                if (
                    clients_repr is not None
                    and limit is not None
                    and 0 < len(clients) < limit
                )
                else message
            )
            logger.log(level, message)

    task = spawn_guarded_background_task(_log)
    task.set_name('relay-server-client-logger')

    return task


async def serve(config: RelayServingConfig) -> None:
    """Run the relay server.

    Initializes a
    [`RelayServer`][proxystore.p2p.relay.server.RelayServer]
    and starts a websocket server listening for new connections
    and incoming messages.

    Note:
        This function will not configure any logging. Configuring logging
        according to
        [`RelayServingConfig.logging`][proxystore.p2p.relay.config.RelayServingConfig]
        is the responsibility of the caller.

    Args:
        config: Serving configuration.
    """
    authenticator = get_authenticator(config.auth)
    server = RelayServer(
        authenticator,
        max_message_bytes=config.max_message_bytes,
    )

    # Set the stop condition when receiving SIGINT (ctrl-C) and SIGTERM.
    loop = asyncio.get_running_loop()
    stop = loop.create_future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
    loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)

    ssl_context: ssl.SSLContext | None = None
    if config.certfile is not None:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_context.load_cert_chain(config.certfile, keyfile=config.keyfile)

    client_logger_task: asyncio.Task[None] | None = None
    if config.logging.current_client_interval is not None:  # pragma: no branch
        level = (
            config.logging.default_level
            if isinstance(config.logging.default_level, int)
            else logging.getLevelName(config.logging.default_level)
        )
        client_logger_task = periodic_client_logger(
            server,
            config.logging.current_client_interval,
            config.logging.current_client_limit,
            level=level,
        )

    config_repr = pprint.pformat(config, indent=2)
    logger.info(f'Relay serving configuration:\n{config_repr}')

    async with websockets.server.serve(
        server.handler,
        config.host,
        config.port,
        logger=None,
        ssl=ssl_context,
    ):
        logger.info(f'Relay server listening on port {config.port}')
        logger.info('Use ctrl-C to stop')
        await stop

    if client_logger_task is not None:  # pragma: no branch
        client_logger_task.cancel()
        try:
            await client_logger_task
        except asyncio.CancelledError:
            pass

    loop.remove_signal_handler(signal.SIGINT)
    loop.remove_signal_handler(signal.SIGTERM)

    logger.info('Relay server shutdown')


@click.command()
@click.option('--config', '-c', 'config_path', help='Configuration file.')
@click.option('--host', metavar='ADDR', help='Interface to bind to.')
@click.option('--port', type=int, metavar='PORT', help='Port to bind to.')
@click.option('--log-dir', metavar='PATH', help='Logging directoryy.')
@click.option(
    '--log-level',
    type=click.Choice(
        ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'],
        case_sensitive=False,
    ),
    help='Minimum logging level.',
)
def cli(
    config_path: str | None,
    host: str | None,
    port: int | None,
    log_dir: str | None,
    log_level: str | None,
) -> None:
    """Run a relay server instance.

    The relay server is used by clients to establish peer-to-peer
    WebRTC connections. If no configuration file is provided, a default
    configuration will be created from
    [`RelayServingConfig()`][proxystore.p2p.relay.config.RelayServingConfig].
    The remaining CLI options will override the options provided in the
    configuration object.
    """
    config = (
        RelayServingConfig()
        if config_path is None
        else RelayServingConfig.from_toml(config_path)
    )

    # Override config with CLI options if given
    if host is not None:
        config.host = host
    if port is not None:
        config.port = port
    if log_dir is not None:
        config.logging.log_dir = log_dir
    if log_level is not None:
        config.logging.default_level = logging.getLevelName(log_level)

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if config.logging.log_dir is not None:
        os.makedirs(config.logging.log_dir, exist_ok=True)
        handlers.append(
            logging.handlers.TimedRotatingFileHandler(
                os.path.join(config.logging.log_dir, 'server.log'),
                # Rotate logs Sunday at midnight
                when='W6',
                atTime=datetime.time(hour=0, minute=0, second=0),
            ),
        )

    logging.basicConfig(
        format=(
            '[%(asctime)s.%(msecs)03d] %(levelname)-5s (%(name)s) :: '
            '%(message)s'
        ),
        datefmt='%Y-%m-%d %H:%M:%S',
        level=config.logging.default_level,
        handlers=handlers,
    )

    logging.getLogger('websockets').setLevel(config.logging.websockets_level)

    asyncio.run(serve(config))
