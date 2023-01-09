"""Endpoint management commands.

These are the implementations of the commands available via the
:any:`proxystore-endpoint <proxystore.endpoint.cli.main>` command.
Subsequently, all commands log errors and results and return status codes
(rather than raising errors and returning results).
"""
from __future__ import annotations

import contextlib
import enum
import logging
import os
import shutil
import signal
import uuid
from typing import Generator

import daemon.pidfile
import psutil

from proxystore import utils
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import get_configs
from proxystore.endpoint.config import get_log_filepath
from proxystore.endpoint.config import get_pid_filepath
from proxystore.endpoint.config import read_config
from proxystore.endpoint.config import write_config
from proxystore.endpoint.serve import serve
from proxystore.utils import home_dir

logger = logging.getLogger(__name__)


class EndpointStatus(enum.Enum):
    """Endpoint status."""

    RUNNING = enum.auto()
    """Endpoint is running on this host."""
    STOPPED = enum.auto()
    """Endpoint is stopped."""
    UNKNOWN = enum.auto()
    """Endpoint cannot be found (missing/corrupted directory)."""
    HANGING = enum.auto()
    """Endpoint PID file exists but process is not active.

    This is either because the process died unexpectedly or the endpoint
    is running on another host.
    """


def get_status(name: str, proxystore_dir: str | None = None) -> EndpointStatus:
    """Check status of endpoint.

    Args:
        name (str): name of endpoint to check.
        proxystore_dir (str): optionally specify the proxystore home directory.
            Defaults to :py:func:`~proxystore.utils.home_dir`.

    Returns:
        :py:attr:`.EndpointStatus.RUNNING` if the endpoint has a valid
        directory and the PID file points to a running process.
        :py:attr:`.EndpointStatus.STOPPED` if the endpoint has a valid
        directory and no PID file.
        :py:attr:`.EndpointStatus.UNKNOWN` if the endpoint directory is
        missing or the config file is missing/unreadable.
        :py:attr:`.EndpointStatus.HANGING` if the endpoint has a valid
        directory but the PID file does not point to a running process. This
        can be due to the endpoint process dying unexpectedly or the endpoint
        process is on a different host.
    """
    if proxystore_dir is None:
        proxystore_dir = home_dir()

    endpoint_dir = os.path.join(proxystore_dir, name)
    if not os.path.isdir(endpoint_dir):
        return EndpointStatus.UNKNOWN

    try:
        read_config(endpoint_dir)
    except (FileNotFoundError, ValueError) as e:
        logger.error(e)
        return EndpointStatus.UNKNOWN

    pid_file = get_pid_filepath(endpoint_dir)
    if not os.path.isfile(pid_file):
        return EndpointStatus.STOPPED

    pid = int(open(pid_file).read().strip())

    if psutil.pid_exists(pid):
        return EndpointStatus.RUNNING
    else:
        return EndpointStatus.HANGING


def configure_endpoint(
    name: str,
    *,
    port: int,
    server: str | None,
    proxystore_dir: str | None = None,
    max_memory: int | None = None,
    dump_dir: str | None = None,
    peer_channels: int = 1,
) -> int:
    """Configure a new endpoint.

    Args:
        name (str): name of endpoint.
        port (int): port for endpoint to listen on.
        server (str): optional address of signaling server for P2P endpoint
            connections.
        proxystore_dir (str): optionally specify the proxystore home directory.
            Defaults to :py:func:`~proxystore.utils.home_dir`.
        max_memory (int): optional max memory in bytes to use for storing
            objects. If exceeded, LRU objects will be dumped to `dump_dir`
            (default: None).
        dump_dir (str): optional directory to dump objects to if the
            memory limit is exceeded (default: None).
        peer_channels (int): number of datachannels per peer connection
            to another endpoint to communicate over (default: 1).

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages
        are logged to the default logger.
    """
    try:
        cfg = EndpointConfig(
            name=name,
            uuid=uuid.uuid4(),
            host=None,
            port=port,
            server=server,
            max_memory=max_memory,
            dump_dir=dump_dir,
            peer_channels=peer_channels,
        )
    except ValueError as e:
        logger.error(str(e))
        return 1

    if proxystore_dir is None:
        proxystore_dir = home_dir()
    endpoint_dir = os.path.join(proxystore_dir, name)

    if os.path.exists(endpoint_dir):
        logger.error(f'An endpoint named {name} already exists.')
        logger.info('To reconfigure the endpoint, remove and try again.')
        return 1

    write_config(cfg, endpoint_dir)

    logger.info(f'Configured endpoint {cfg.name} <{cfg.uuid}>. Start with:')
    logger.info(f'  $ proxystore-endpoint start {cfg.name}')

    return 0


def list_endpoints(
    *,
    proxystore_dir: str | None = None,
) -> int:
    """List available endpoints.

    Args:
        proxystore_dir (str): optionally specify the proxystore home directory.
            Defaults to :py:func:`~proxystore.utils.home_dir`.

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages
        are logged to the default logger.
    """
    if proxystore_dir is None:
        proxystore_dir = home_dir()

    endpoints = get_configs(proxystore_dir)

    if len(endpoints) == 0:
        logger.info(f'No valid endpoint configurations in {proxystore_dir}.')
    else:
        eps = [(e.name, str(e.uuid)) for e in endpoints]
        eps = sorted(eps, key=lambda x: x[0])
        logger.info(f'{"NAME":<18} {"STATUS":<8} UUID', extra={'simple': True})
        logger.info('=' * (19 + 9 + len(eps[0][1])), extra={'simple': True})
        for name, uuid_ in eps:
            status = get_status(name, proxystore_dir)
            logger.info(
                f'{name:18.18} {status.name:<8.8} {uuid_}',
                extra={'simple': True},
            )

    return 0


def remove_endpoint(
    name: str,
    *,
    proxystore_dir: str | None = None,
) -> int:
    """Remove endpoint.

    Args:
        name (str): name of endpoint to remove.
        proxystore_dir (str): optionally specify the proxystore home directory.
            Defaults to :py:func:`~proxystore.utils.home_dir`.

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages
        are logged to the default logger.
    """
    if proxystore_dir is None:
        proxystore_dir = home_dir()
    endpoint_dir = os.path.join(proxystore_dir, name)

    if not os.path.exists(endpoint_dir):
        logger.error(f'An endpoint named {name} does not exist.')
        return 1

    status = get_status(name, proxystore_dir)
    if status in (EndpointStatus.RUNNING, EndpointStatus.HANGING):
        logger.error('Endpoint must be stopped before removing.')
        logger.error(f'  $ proxystore-endpoint stop {name}')
        return 1

    shutil.rmtree(endpoint_dir)

    logger.info(f'Removed endpoint named {name}.')

    return 0


def start_endpoint(
    name: str,
    *,
    detach: bool = False,
    log_level: str = 'INFO',
    proxystore_dir: str | None = None,
) -> int:
    """Start endpoint.

    Args:
        name (str): name of endpoint to start.
        detach (bool): start the endpoint as a daemon process.
        log_level (str): set logging level of endpoint.
        proxystore_dir (str): optionally specify the proxystore home directory.
            Defaults to :py:func:`~proxystore.utils.home_dir`.

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages
        are logged to the default logger.
    """
    if proxystore_dir is None:
        proxystore_dir = home_dir()

    status = get_status(name, proxystore_dir)
    if status == EndpointStatus.RUNNING:
        logger.error(f'Endpoint {name} is already running.')
        return 1
    elif status == EndpointStatus.UNKNOWN:
        logger.error(f'A valid endpoint named {name} does not exist.')
        logger.error('Use `list` to see available endpoints.')
        return 1

    endpoint_dir = os.path.join(proxystore_dir, name)
    cfg = read_config(endpoint_dir)
    hostname = utils.hostname()

    pid_file = get_pid_filepath(endpoint_dir)

    if (
        status == EndpointStatus.HANGING
        and cfg.host is not None
        and hostname != cfg.host
    ):
        logger.error(
            'A PID file exists for the endpoint, but the config indicates the '
            f'endpoint is running on a host named {cfg.host}. Try stopping '
            f'the endpoint on {cfg.host}. Otherwise, delete the PID file at '
            f'{pid_file} and try again.',
        )
        return 1
    elif status == EndpointStatus.HANGING:
        logger.debug(f'Removing invalid PID file ({pid_file}).')
        os.remove(pid_file)

    # Write out new config with host so clients can see the current host
    cfg.host = hostname
    write_config(cfg, endpoint_dir)

    log_file = get_log_filepath(endpoint_dir)

    if detach:
        logger.info('Starting endpoint process as daemon.')
        logger.info(f'Logs will be written to {log_file}')

        context = daemon.DaemonContext(
            working_directory=endpoint_dir,
            umask=0o002,
            pidfile=daemon.pidfile.PIDLockFile(pid_file),
            detach_process=True,
            # Note: stdin, stdout, stderr left as None which binds to /dev/null
        )
    else:
        context = _attached_pid_manager(pid_file)

    # TODO: handle sigterm/sigkill exit codes/graceful shutdown.
    with context:
        serve(cfg, log_level=log_level, log_file=log_file)

    return 0


def stop_endpoint(name: str, *, proxystore_dir: str | None = None) -> int:
    """Stop endpoint.

    Args:
        name (str): name of endpoint to start.
        proxystore_dir (str): optionally specify the proxystore home directory.
            Defaults to :py:func:`~proxystore.utils.home_dir`.

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages
        are logged to the default logger.
    """
    if proxystore_dir is None:
        proxystore_dir = home_dir()

    status = get_status(name, proxystore_dir)
    if status == EndpointStatus.UNKNOWN:
        logger.error(f'A valid endpoint named {name} does not exist.')
        logger.error('Use `list` to see available endpoints.')
        return 1
    elif status == EndpointStatus.STOPPED:
        logger.info(f'Endpoint {name} is not running.')
        return 0

    endpoint_dir = os.path.join(proxystore_dir, name)
    cfg = read_config(endpoint_dir)
    hostname = utils.hostname()
    pid_file = get_pid_filepath(endpoint_dir)

    if (
        status == EndpointStatus.HANGING
        and cfg.host is not None
        and hostname != cfg.host
    ):
        logger.error(
            'A PID file exists for the endpoint, but the config indicates the '
            f'endpoint is running on a host named {cfg.host}. Try stopping '
            f'the endpoint on {cfg.host}. Otherwise, delete the PID file at '
            f'{pid_file} and try again.',
        )
        return 1
    elif status == EndpointStatus.HANGING:
        logger.debug(f'Removing invalid PID file ({pid_file}).')
        os.remove(pid_file)
        logger.info(f'Endpoint {name} is not running.')
        return 0

    assert status == EndpointStatus.RUNNING
    with open(pid_file) as f:
        pid = int(f.read().strip())

    logger.debug(f'Terminating endpoint process (PID: {pid}).')
    # Source: https://github.com/funcx-faas/funcX/blob/facf37348f9a9eb4e1a0572793d7b6819be5754d/funcx_endpoint/funcx_endpoint/endpoint/endpoint.py#L360  # noqa: E501
    parent = psutil.Process(pid)
    processes = parent.children(recursive=True)
    processes.append(parent)
    for p in processes:
        p.send_signal(signal.SIGTERM)

    terminated, alive = psutil.wait_procs(processes, timeout=1)
    for p in alive:  # pragma: no cover
        try:
            p.send_signal(signal.SIGKILL)
        except psutil.NoSuchProcess:
            pass

    if os.path.isfile(pid_file):  # pragma: no branch
        logger.debug(f'Cleaning up PID file ({pid_file}).')
        os.remove(pid_file)

    logger.info(f'Endpoint {name} has been stopped.')
    return 0


@contextlib.contextmanager
def _attached_pid_manager(pid_file: str) -> Generator[None, None, None]:
    """Context manager that writes and cleans up a PID file."""
    with open(pid_file, 'w') as f:
        f.write(str(os.getpid()))
    yield
    os.remove(pid_file)
