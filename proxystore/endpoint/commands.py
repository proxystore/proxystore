"""Endpoint management commands.

These are the implementations of the commands available via the
[`proxystore-endpoint`](../cli.md#proxystore-endpoint) command.
Subsequently, all commands log errors and results and return status codes
(rather than raising errors and returning results).
"""

from __future__ import annotations

import contextlib
import enum
import logging
import os
import random
import shutil
import signal
import socket
import time
import uuid
from collections.abc import Generator
from typing import Literal

import daemon.pidfile

from proxystore import utils
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import EndpointStorageConfig
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.directory import is_own_process
from proxystore.endpoint.serve import serve
from proxystore.utils.environment import home_dir

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
        name: Name of endpoint to check.
        proxystore_dir: Optionally specify the proxystore home directory.
            Defaults to [`home_dir()`][proxystore.utils.environment.home_dir].

    Returns:
        `EndpointStatus.RUNNING` if the endpoint has a valid directory and \
        the PID file points to a running process. \
        `EndpointStatus.STOPPED` if the endpoint has a valid directory and no \
        PID file. \
        `EndpointStatus.UNKNOWN` if the endpoint directory is missing or the \
        config file is missing/unreadable. \
        `EndpointStatus.HANGING` if the endpoint has a valid directory but \
        the PID file does not point to a running process. This can be due to \
        the endpoint process dying unexpectedly or the endpoint process is on \
        a different host.
    """
    if proxystore_dir is None:
        proxystore_dir = home_dir()

    endpoint_dir = EndpointDir.from_home(proxystore_dir, name)
    if not os.path.isdir(endpoint_dir):
        return EndpointStatus.UNKNOWN

    try:
        endpoint_dir.read_config()
    except (FileNotFoundError, ValueError) as e:
        logger.error(e)
        return EndpointStatus.UNKNOWN

    pid_file = endpoint_dir.pid_path
    if not os.path.isfile(pid_file):
        return EndpointStatus.STOPPED

    with open(pid_file) as f:
        pid = int(f.read().strip())

    if is_own_process(pid):
        return EndpointStatus.RUNNING
    return EndpointStatus.HANGING


def configure_endpoint(
    name: str,
    *,
    host: str = 'ip',
    persist_data: bool = False,
    port: int | None,
    proxystore_dir: str | None = None,
    tls: bool = False,
) -> int:
    """Configure a new endpoint.

    Args:
        name: Name of endpoint.
        host: Method to resolve the hostname of the endpoint ("ip" or "fqdn")
            or a static address to use.
        persist_data: Persist data stored in the endpoint.
        port: Port for endpoint to listen on. If `None`, a random port is
            selected.
        proxystore_dir: Optionally specify the proxystore home directory.
            Defaults to [`home_dir()`][proxystore.utils.environment.home_dir].
        tls: Encrypt connections between clients and the endpoint with TLS.

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages \
        are logged to the default logger.
    """
    if proxystore_dir is None:
        proxystore_dir = home_dir()
    endpoint_dir = EndpointDir.from_home(proxystore_dir, name)

    database_path = endpoint_dir.database_path if persist_data else None

    host_addr: str | None = None
    host_type: Literal['fqdn', 'ip', 'static']
    if host.lower().strip() == 'fqdn':
        host_type = 'fqdn'
    elif host.lower().strip() == 'ip':
        host_type = 'ip'
    else:
        host_addr = host
        host_type = 'static'

    port = port if port is not None else random.randint(10 * 1024, 20 * 1024)

    try:
        cfg = EndpointConfig(
            name=name,
            uuid=str(uuid.uuid4()),
            host=host_addr,
            port=port,
            host_type=host_type,
            tls=tls,
            storage=EndpointStorageConfig(database_path=database_path),
        )
    except ValueError as e:
        logger.error(str(e))
        return 1

    if os.path.exists(endpoint_dir):
        logger.error('An endpoint named %s already exists.', name)
        logger.info('To reconfigure the endpoint, remove and try again.')
        return 1

    endpoint_dir.write_config(cfg)

    logger.info('Configured endpoint: %s <%s>', cfg.name, cfg.uuid)
    logger.info('Config and log file directory: %s', endpoint_dir)
    logger.info('Start the endpoint with:')
    logger.info('  $ proxystore-endpoint start %s', cfg.name)

    return 0


def list_endpoints(
    *,
    proxystore_dir: str | None = None,
) -> int:
    """List available endpoints.

    Args:
        proxystore_dir: Optionally specify the proxystore home directory.
            Defaults to [`home_dir()`][proxystore.utils.environment.home_dir].

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages \
        are logged to the default logger.
    """
    if proxystore_dir is None:
        proxystore_dir = home_dir()

    endpoints = [c for _, c in EndpointDir.find_all(proxystore_dir)]

    max_status_chars = max(
        len('STATUS'),
        *(len(e.name) for e in EndpointStatus),
    )
    # Note: endpoints can be empty so we need to pass an iterable rather
    # than unpacking the arguments
    max_endpoint_chars = max([18] + [len(e.name) for e in endpoints])

    if len(endpoints) == 0:
        logger.info('No valid endpoint configurations in %s.', proxystore_dir)
        return 0

    eps = [(e.name, str(e.uuid)) for e in endpoints]
    eps = sorted(eps, key=lambda x: x[0])
    logger.info(
        '%-*s %-*s UUID',
        max_endpoint_chars,
        'NAME',
        max_status_chars,
        'STATUS',
        extra={'simple': True},
    )

    toprule_len = 2 + max_endpoint_chars + max_status_chars + len(eps[0][1])
    logger.info('=' * toprule_len, extra={'simple': True})

    for name, uuid_ in eps:
        status = get_status(name, proxystore_dir)
        logger.info(
            '%-*.*s %-*.*s %s',
            max_endpoint_chars,
            max_endpoint_chars,
            name,
            max_status_chars,
            max_status_chars,
            status.name,
            uuid_,
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
        name: Name of endpoint to remove.
        proxystore_dir: Optionally specify the proxystore home directory.
            Defaults to [`home_dir()`][proxystore.utils.environment.home_dir].

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages \
        are logged to the default logger.
    """
    if proxystore_dir is None:
        proxystore_dir = home_dir()
    endpoint_dir = EndpointDir.from_home(proxystore_dir, name)

    if not os.path.exists(endpoint_dir):
        logger.error('An endpoint named %s does not exist.', name)
        return 1

    status = get_status(name, proxystore_dir)
    if status in (EndpointStatus.RUNNING, EndpointStatus.HANGING):
        logger.error('Endpoint must be stopped before removing.')
        logger.error('  $ proxystore-endpoint stop %s', name)
        return 1

    shutil.rmtree(endpoint_dir)

    logger.info('Removed endpoint named %s.', name)

    return 0


def start_endpoint(  # noqa: C901
    name: str,
    *,
    detach: bool = False,
    log_level: str = 'INFO',
    proxystore_dir: str | None = None,
) -> int:
    """Start endpoint.

    Args:
        name: Name of endpoint to start.
        detach: Start the endpoint as a daemon process.
        log_level: Logging level of the endpoint.
        proxystore_dir: Optionally specify the proxystore home directory.
            Defaults to [`home_dir()`][proxystore.utils.environment.home_dir].

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages \
        are logged to the default logger.
    """
    if proxystore_dir is None:
        proxystore_dir = home_dir()

    status = get_status(name, proxystore_dir)
    if status == EndpointStatus.RUNNING:
        logger.error('Endpoint %s is already running.', name)
        return 1
    if status == EndpointStatus.UNKNOWN:
        logger.error('A valid endpoint named %s does not exist.', name)
        logger.error('Use `list` to see available endpoints.')
        return 1

    endpoint_dir = EndpointDir.from_home(proxystore_dir, name)
    cfg = endpoint_dir.read_config()

    if cfg.host_type == 'fqdn':
        hostname = socket.getfqdn()
    elif cfg.host_type == 'ip':
        hostname = socket.gethostbyname(utils.hostname())
    elif cfg.host_type == 'static' and cfg.host is None:
        path = endpoint_dir.config_path
        logger.error('Missing static host address in config.')
        logger.error(
            'Set the `host` field or change the `host_type` to '
            '"ip" or "fqdn" in the config.',
        )
        logger.error('  Config: %s', path)
        return 1
    elif cfg.host_type == 'static' and cfg.host is not None:
        hostname = cfg.host
    else:
        raise AssertionError('Unreachable.')

    pid_file = endpoint_dir.pid_path

    if (
        status == EndpointStatus.HANGING
        and cfg.host is not None
        and hostname != cfg.host
    ):
        logger.error(
            'A PID file exists for the endpoint, but the config indicates the '
            'endpoint is running on a host named %s. Try stopping '
            'the endpoint on %s. Otherwise, delete the PID file at '
            '%s and try again.',
            cfg.host,
            cfg.host,
            pid_file,
        )
        return 1
    if status == EndpointStatus.HANGING:
        logger.debug('Removing invalid PID file (%s).', pid_file)
        os.remove(pid_file)

    # Write out new config with host so clients can see the current host
    cfg.host = hostname
    endpoint_dir.write_config(cfg)

    log_file = endpoint_dir.log_path

    if detach:
        logger.info('Starting endpoint process as daemon.')
        logger.info('Logs will be written to %s', log_file)

        context = daemon.DaemonContext(
            working_directory=endpoint_dir.path,
            umask=0o077,
            pidfile=daemon.pidfile.PIDLockFile(pid_file),
            detach_process=True,
            # Note: stdin, stdout, stderr left as None which binds to /dev/null
        )
    else:
        context = _attached_pid_manager(pid_file)

    with context:
        # Note: serve will handle most interrupts which can be reasonably
        # handled and return gracefully.
        serve(
            endpoint_dir,
            log_level=log_level,
            log_file=log_file,
        )

    return 0


def stop_endpoint(name: str, *, proxystore_dir: str | None = None) -> int:
    """Stop endpoint.

    Args:
        name: Name of endpoint to stop.
        proxystore_dir: Optionally specify the proxystore home directory.
            Defaults to [`home_dir()`][proxystore.utils.environment.home_dir].

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages \
        are logged to the default logger.
    """
    if proxystore_dir is None:
        proxystore_dir = home_dir()

    status = get_status(name, proxystore_dir)
    if status == EndpointStatus.UNKNOWN:
        logger.error('A valid endpoint named %s does not exist.', name)
        logger.error('Use `list` to see available endpoints.')
        return 1
    if status == EndpointStatus.STOPPED:
        logger.info('Endpoint %s is not running.', name)
        return 0

    endpoint_dir = EndpointDir.from_home(proxystore_dir, name)
    cfg = endpoint_dir.read_config()
    hostname = utils.hostname()
    pid_file = endpoint_dir.pid_path

    if (
        status == EndpointStatus.HANGING
        and cfg.host is not None
        and hostname != cfg.host
    ):
        logger.error(
            'A PID file exists for the endpoint, but the config indicates the '
            'endpoint is running on a host named %s. Try stopping '
            'the endpoint on %s. Otherwise, delete the PID file at '
            '%s and try again.',
            cfg.host,
            cfg.host,
            pid_file,
        )
        return 1
    if status == EndpointStatus.HANGING:
        logger.debug('Removing invalid PID file (%s).', pid_file)
        os.remove(pid_file)
        logger.info('Endpoint %s is not running.', name)
        return 0

    assert status == EndpointStatus.RUNNING
    with open(pid_file) as f:
        pid = int(f.read().strip())

    logger.debug('Terminating endpoint process (PID: %s).', pid)
    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, signal.SIGTERM)

    if not _wait_for_exit(pid, timeout=1):  # pragma: no cover
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)

    if os.path.isfile(pid_file):  # pragma: no branch
        logger.debug('Cleaning up PID file (%s).', pid_file)
        os.remove(pid_file)

    logger.info('Endpoint %s has been stopped.', name)
    return 0


@contextlib.contextmanager
def _attached_pid_manager(pid_file: str) -> Generator[None, None, None]:
    """Context manager that writes and cleans up a PID file."""
    with open(pid_file, 'w') as f:
        f.write(str(os.getpid()))
    try:
        yield
    finally:
        os.remove(pid_file)


def _wait_for_exit(pid: int, timeout: float) -> bool:
    """Wait for a process to exit.

    Returns:
        `True` if the process exited before the timeout.
    """
    deadline = time.monotonic() + timeout
    while True:
        # Reap the process if it is a child of this process. This only
        # happens in tests; otherwise the zombie would appear alive.
        with contextlib.suppress(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)
        if not is_own_process(pid):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.01)
