"""ProxyStore endpoint commands.

These are the implementations of the commands available via the
:code:`proxystore-endpoint` command. Subsequently, all commands log errors
and results (rather than raising errors and returning results) and return
status codes.
"""
from __future__ import annotations

import logging
import os
import re
import shutil
import uuid

from proxystore.endpoint.config import default_dir
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import get_configs
from proxystore.endpoint.config import read_config
from proxystore.endpoint.config import write_config
from proxystore.endpoint.serve import serve

logger = logging.getLogger(__name__)


def _validate_name(name: str) -> bool:
    """Validate name only contains alphanumeric or dash/underscore chars."""
    return len(re.findall(r'[^A-Za-z0-9_\-]', name)) == 0 and len(name) > 0


def configure_endpoint(
    name: str,
    *,
    host: str,
    port: int,
    server: str | None,
    proxystore_dir: str | None = None,
) -> int:
    """Configure a new endpoint.

    Args:
        name (str): name of endpoint.
        host (str): IP address of host the endpoint will be run on.
        port (int): port for endpoint to listen on.
        server (str): optional address of singaling server for P2P endpoint
            connections.
        proxystore_dir (str): optionally specify the directory where endpoint
            configurations are saved. Defaults to :code:`$HOME/.proxystore`.

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages
        are logged to the default logger.
    """
    if not _validate_name(name):
        logger.error(
            'Names must only contain alphanumeric characters, dashes, and '
            ' underscores.',
        )
        return 1

    if proxystore_dir is None:
        proxystore_dir = default_dir()
    endpoint_dir = os.path.join(proxystore_dir, name)

    if os.path.exists(endpoint_dir):
        logger.error(f'An endpoint named {name} already exists. ')
        logger.error('To reconfigure the endpoint, remove and try again.')
        return 1

    cfg = EndpointConfig(
        name=name,
        uuid=str(uuid.uuid4()),
        host=host,
        port=port,
        server=server,
    )
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
        eps = [(e.name, e.uuid) for e in endpoints]
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

    # Update logger for serve() in case caller already had configured logger.
    # TODO: create new logger object to pass to serve.
    logging.basicConfig(level=log_level)

    # TODO: handle sigterm/sigkill exit codes/graceful shutdown.
    serve(
        name=cfg.name,
        uuid=cfg.uuid,
        host=cfg.host,
        port=cfg.port,
        server=cfg.server,
    )

    return 0
