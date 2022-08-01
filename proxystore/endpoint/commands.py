"""ProxyStore endpoint commands.

These are the implementations of the commands available via the
:any:`proxystore-endpoint <proxystore.endpoint.cli.main>` command.
Subsequently, all commands log errors and results and return status codes
(rather than raising errors and returning results).
"""
from __future__ import annotations

import logging
import os
import shutil
import uuid

from proxystore.endpoint.config import default_dir
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import get_configs
from proxystore.endpoint.config import read_config
from proxystore.endpoint.config import write_config
from proxystore.endpoint.serve import serve

logger = logging.getLogger(__name__)


def configure_endpoint(
    name: str,
    *,
    host: str,
    port: int,
    server: str | None,
    proxystore_dir: str | None = None,
    max_memory: int | None = None,
    dump_dir: str | None = None,
) -> int:
    """Configure a new endpoint.

    Args:
        name (str): name of endpoint.
        host (str): IP address of host the endpoint will be run on.
        port (int): port for endpoint to listen on.
        server (str): optional address of signaling server for P2P endpoint
            connections.
        proxystore_dir (str): optionally specify the directory where endpoint
            configurations are saved. Defaults to :code:`$HOME/.proxystore`.
        max_memory (int): optional max memory in bytes to use for storing
            objects. If exceeded, LRU objects will be dumped to `dump_dir`
            (default: None).
        dump_dir (str): optional directory to dump objects to if the
            memory limit is exceeded (default: None).

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages
        are logged to the default logger.
    """
    try:
        cfg = EndpointConfig(
            name=name,
            uuid=uuid.uuid4(),
            host=host,
            port=port,
            server=server,
            max_memory=max_memory,
            dump_dir=dump_dir,
        )
    except ValueError as e:
        logger.error(str(e))
        return 1

    if proxystore_dir is None:
        proxystore_dir = default_dir()
    endpoint_dir = os.path.join(proxystore_dir, name)

    if os.path.exists(endpoint_dir):
        logger.error(f'An endpoint named {name} already exists. ')
        logger.error('To reconfigure the endpoint, remove and try again.')
        return 1

    write_config(cfg, endpoint_dir)

    logger.info(f'Configured endpoint {cfg.name} <{cfg.uuid}>.')
    logger.info('')
    logger.info('To start the endpoint:')
    logger.info(f'  $ proxystore-endpoint start {cfg.name}.')

    return 0


def list_endpoints(
    *,
    proxystore_dir: str | None = None,
) -> int:
    """List available endpoints.

    Args:
        proxystore_dir (str): optionally specify the directory to look in for
            endpoint configurations. Defaults to :code:`$HOME/.proxystore`.

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages
        are logged to the default logger.
    """
    if proxystore_dir is None:
        proxystore_dir = default_dir()

    endpoints = get_configs(proxystore_dir)

    if len(endpoints) == 0:
        logger.info(f'No valid endpoint configurations in {proxystore_dir}.')
    else:
        eps = [(e.name, str(e.uuid)) for e in endpoints]
        eps = sorted(eps, key=lambda x: x[0])
        logger.info(f'{"NAME":<18} UUID')
        logger.info('=' * (18 + 1 + len(eps[0][1])))
        for name, uuid_ in eps:
            logger.info(f'{name:<18} {uuid_}')

    return 0


def remove_endpoint(
    name: str,
    *,
    proxystore_dir: str | None = None,
) -> int:
    """Remove endpoint.

    Args:
        name (str): name of endpoint to remove.
        proxystore_dir (str): optionally specify the directory where the
            endpoint configuration is. Defaults to :code:`$HOME/.proxystore`.

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages
        are logged to the default logger.
    """
    if proxystore_dir is None:
        proxystore_dir = default_dir()
    endpoint_dir = os.path.join(proxystore_dir, name)

    if not os.path.exists(endpoint_dir):
        logger.error(f'An endpoint named {name} does not exist.')
        return 1

    shutil.rmtree(endpoint_dir)

    logger.info(f'Removed endpoint named {name}.')

    return 0


def start_endpoint(
    name: str,
    *,
    log_level: str = 'INFO',
    proxystore_dir: str | None = None,
) -> int:
    """Start endpoint.

    Args:
        name (str): name of endpoint to start.
        log_level (str): set logging level of endpoint.
        proxystore_dir (str): optionally specify the directory where the
            endpoint configuration is. Defaults to :code:`$HOME/.proxystore`.

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages
        are logged to the default logger.
    """
    if proxystore_dir is None:
        proxystore_dir = default_dir()

    endpoint_dir = os.path.join(proxystore_dir, name)
    if not os.path.exists(endpoint_dir):
        logger.error(f'An endpoint named {name} does not exist.')
        logger.error('Use `list` to see available endpoints.')
        return 1

    try:
        cfg = read_config(endpoint_dir)
    except FileNotFoundError:
        logger.error(
            f'{os.path.join(proxystore_dir, name)} does not have a '
            'config file.',
        )
        logger.error('Try removing the endpoint and configuring it again.')
        return 1
    except ValueError as e:
        logger.error(str(e))
        logger.error('Correct the endpoint config and try again.')
        return 1

    # TODO: handle sigterm/sigkill exit codes/graceful shutdown.
    serve(
        name=cfg.name,
        uuid=cfg.uuid,
        host=cfg.host,
        port=cfg.port,
        server=cfg.server,
        log_level=log_level,
        log_file=os.path.join(endpoint_dir, 'endpoint.log'),
        max_memory=cfg.max_memory,
        dump_dir=cfg.dump_dir,
    )

    return 0
