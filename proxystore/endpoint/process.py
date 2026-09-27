"""Start and stop endpoint processes.

An endpoint runs in its own process, optionally as a daemon, and records its
PID in the `daemon.pid` file of its directory. These functions manage that
process and raise
[`EndpointError`][proxystore.endpoint.exceptions.EndpointError] subclasses on
failure.

Note:
    This module requires the `endpoints` extra.
"""

from __future__ import annotations

import contextlib
import logging
import os
import signal
import time
from collections.abc import Generator

import daemon.pidfile

from proxystore import utils
from proxystore.endpoint.config import resolve_host
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.directory import EndpointStatus
from proxystore.endpoint.directory import is_own_process
from proxystore.endpoint.exceptions import EndpointConfigError
from proxystore.endpoint.exceptions import EndpointNotFoundError
from proxystore.endpoint.exceptions import EndpointRunningError
from proxystore.endpoint.serve import serve

logger = logging.getLogger(__name__)


def start_endpoint(
    endpoint_dir: EndpointDir,
    *,
    detach: bool = False,
    log_level: int | str = logging.INFO,
) -> None:
    """Start an endpoint.

    Warning:
        If `detach` is `False`, this function does not return until the
        endpoint receives SIGINT or SIGTERM.

    Args:
        endpoint_dir: Directory of the endpoint to start.
        detach: Start the endpoint as a daemon process.
        log_level: Logging level of the endpoint.

    Raises:
        EndpointNotFoundError: If the endpoint does not exist.
        EndpointConfigError: If the configuration is invalid or the host
            address cannot be resolved.
        EndpointRunningError: If the endpoint is already running on this or
            another host.
    """
    status = _status(endpoint_dir)
    if status == EndpointStatus.RUNNING:
        raise EndpointRunningError(
            f'Endpoint {_name(endpoint_dir)} is already running.',
        )

    # Resolve the host before daemonizing so errors are raised to the
    # caller rather than only written to the log.
    host = endpoint_dir.read_config().host
    try:
        resolve_host(host)
    except OSError as e:
        raise EndpointConfigError(
            f'Unable to resolve the host address ({host}): {e}',
        ) from e

    if status == EndpointStatus.HANGING:
        _check_not_running_elsewhere(endpoint_dir)
        logger.debug('Removing invalid PID file (%s)', endpoint_dir.pid_path)
        os.remove(endpoint_dir.pid_path)

    context: contextlib.AbstractContextManager[object]
    if detach:
        logger.info('Starting endpoint process as daemon')
        logger.info('Logs will be written to %s', endpoint_dir.log_path)
        context = daemon.DaemonContext(
            working_directory=endpoint_dir.path,
            umask=0o077,
            pidfile=daemon.pidfile.PIDLockFile(endpoint_dir.pid_path),
            detach_process=True,
            # Note: stdin, stdout, stderr left as None which binds to /dev/null
        )
    else:
        context = _attached_pid_manager(endpoint_dir.pid_path)

    with context:
        # Note: serve will handle most interrupts which can be reasonably
        # handled and return gracefully.
        serve(
            endpoint_dir,
            log_level=log_level,
            log_file=endpoint_dir.log_path,
        )


def stop_endpoint(endpoint_dir: EndpointDir) -> bool:
    """Stop an endpoint running on this host.

    Args:
        endpoint_dir: Directory of the endpoint to stop.

    Returns:
        `True` if the endpoint was running and was stopped, or `False` if \
        the endpoint was not running.

    Raises:
        EndpointNotFoundError: If the endpoint does not exist.
        EndpointRunningError: If the endpoint is running on another host.
    """
    status = _status(endpoint_dir)
    if status == EndpointStatus.STOPPED:
        return False
    if status == EndpointStatus.HANGING:
        _check_not_running_elsewhere(endpoint_dir)
        logger.debug('Removing invalid PID file (%s)', endpoint_dir.pid_path)
        os.remove(endpoint_dir.pid_path)
        return False

    pid = endpoint_dir.running_pid()
    assert pid is not None
    logger.debug('Terminating endpoint process (PID: %s)', pid)
    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, signal.SIGTERM)

    if not _wait_for_exit(pid, timeout=1):  # pragma: no cover
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)

    with contextlib.suppress(FileNotFoundError):
        os.remove(endpoint_dir.pid_path)
    return True


def _name(endpoint_dir: EndpointDir) -> str:
    return os.path.basename(endpoint_dir.path)


def _status(endpoint_dir: EndpointDir) -> EndpointStatus:
    status = endpoint_dir.status()
    if status == EndpointStatus.UNKNOWN:
        # Raise the specific reason (i.e., missing or invalid config).
        if not os.path.isdir(endpoint_dir.path):
            raise EndpointNotFoundError(
                f'An endpoint named {_name(endpoint_dir)} does not exist in '
                f'{os.path.dirname(endpoint_dir.path)}.',
            )
        endpoint_dir.read_config()
    return status


def _check_not_running_elsewhere(endpoint_dir: EndpointDir) -> None:
    """Check that the endpoint is not running on a different machine.

    The PID file only identifies a process on the machine that wrote it, so
    the connection file of the running endpoint is used to find the machine.

    Raises:
        EndpointRunningError: If the endpoint is running on another host.
    """
    try:
        info = endpoint_dir.read_connection()
    except (FileNotFoundError, ValueError):
        return
    if info.hostname == utils.hostname():
        return
    raise EndpointRunningError(
        f'Endpoint {_name(endpoint_dir)} appears to be running on '
        f'{info.hostname} (PID {info.pid}). Stop the endpoint on '
        f'{info.hostname}. If it is not running, delete the PID file at '
        f'{endpoint_dir.pid_path} and try again.',
    )


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
