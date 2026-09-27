"""Run, start, and stop endpoint processes.

[`serve()`][proxystore.endpoint.process.serve] runs an
[`Endpoint`][proxystore.endpoint.endpoint.Endpoint] in the current process
until it receives a signal.
[`start_endpoint()`][proxystore.endpoint.process.start_endpoint] runs an
endpoint in this process or as a daemon, and
[`stop_endpoint()`][proxystore.endpoint.process.stop_endpoint] stops the
process of a running endpoint using the PID in its connection file. These
functions raise
[`EndpointError`][proxystore.endpoint.exceptions.EndpointError] subclasses
on failure.

Note:
    This module requires the `endpoints` extra.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import time

import daemon
import uvloop

from proxystore.endpoint.config import resolve_host
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.directory import EndpointStatus
from proxystore.endpoint.directory import is_own_process
from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.exceptions import EndpointConfigError
from proxystore.endpoint.exceptions import EndpointRunningError

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
    # These checks are repeated by the endpoint when it starts but are
    # checked first so errors are raised to the caller rather than only
    # written to the log of a daemon.
    status = _status(endpoint_dir)
    if status == EndpointStatus.RUNNING:
        raise EndpointRunningError(
            f'Endpoint {endpoint_dir.name} is already running.',
        )
    if status == EndpointStatus.OTHER_HOST:
        endpoint_dir.check_not_running_elsewhere()

    host = endpoint_dir.read_config().host
    try:
        resolve_host(host)
    except OSError as e:
        raise EndpointConfigError(
            f'Unable to resolve the host address ({host}): {e}',
        ) from e

    if status == EndpointStatus.STALE:
        logger.debug(
            'Removing stale connection file (%s)',
            endpoint_dir.connection_path,
        )
        endpoint_dir.remove_connection()

    context: contextlib.AbstractContextManager[object]
    if detach:
        logger.info('Starting endpoint process as daemon')
        logger.info('Logs will be written to %s', endpoint_dir.log_path)
        context = daemon.DaemonContext(
            working_directory=endpoint_dir.path,
            umask=0o077,
            detach_process=True,
            # Note: stdin, stdout, stderr left as None which binds to /dev/null
        )
    else:
        context = contextlib.nullcontext()

    with context:
        # Logging is configured after daemonizing because the daemon closes
        # all open files (e.g., the log file).
        configure_logging(log_level, endpoint_dir.log_path)
        # Note: serve will handle most interrupts which can be reasonably
        # handled and return gracefully.
        serve(endpoint_dir)


def configure_logging(
    log_level: int | str = logging.INFO,
    log_file: str | None = None,
) -> None:
    """Configure logging of an endpoint process.

    This sets the level and format of the root logger and optionally
    appends the log to a file. This is only called by
    [`start_endpoint()`][proxystore.endpoint.process.start_endpoint] because
    it changes the logging of the entire process.

    Args:
        log_level: Logging level.
        log_file: Optional file path to append the log to. The parent
            directory is created if it does not exist.
    """
    root = logging.getLogger()
    if log_file is not None:
        os.makedirs(os.path.dirname(log_file) or '.', exist_ok=True)
        root.addHandler(logging.FileHandler(log_file))

    formatter = logging.Formatter(
        '[%(asctime)s.%(msecs)03d] %(levelname)-5s (%(name)s) :: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    for handler in root.handlers:
        handler.setFormatter(formatter)
    root.setLevel(log_level)


def serve(endpoint_dir: EndpointDir, *, use_uvloop: bool = True) -> None:
    """Run an endpoint in the current process.

    Warning:
        This function does not return until the process receives SIGINT or
        SIGTERM.

    Args:
        endpoint_dir: Directory of the endpoint with its configuration. The
            connection file is written to this directory while the
            endpoint is running.
        use_uvloop: Use uvloop as the event loop implementation.
    """
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


async def _serve_async(endpoint_dir: EndpointDir) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    signals = (signal.SIGINT, signal.SIGTERM)
    # Signal handlers are installed before starting the endpoint so that a
    # signal received during start up stops the endpoint once it starts.
    for sig in signals:
        loop.add_signal_handler(sig, stop.set)
    try:
        async with Endpoint(endpoint_dir):
            await stop.wait()
    finally:
        for sig in signals:
            loop.remove_signal_handler(sig)


def stop_endpoint(endpoint_dir: EndpointDir, *, timeout: float = 5) -> bool:
    """Stop an endpoint running on this host.

    The endpoint is sent SIGTERM and killed if it does not exit within
    `timeout` seconds.

    Args:
        endpoint_dir: Directory of the endpoint to stop.
        timeout: Seconds to wait for the endpoint to exit.

    Returns:
        `True` if the endpoint was running and was stopped, or `False` if \
        the endpoint was not running.

    Raises:
        EndpointNotFoundError: If the endpoint does not exist.
        EndpointRunningError: If the endpoint may be running on another host
            or is still starting.
    """
    status = _status(endpoint_dir)
    if status == EndpointStatus.STOPPED:
        return False
    if status == EndpointStatus.OTHER_HOST:
        endpoint_dir.check_not_running_elsewhere()
    if status == EndpointStatus.STALE:
        logger.debug(
            'Removing stale connection file (%s)',
            endpoint_dir.connection_path,
        )
        endpoint_dir.remove_connection()
        return False

    try:
        info = endpoint_dir.read_connection()
    except FileNotFoundError:
        raise EndpointRunningError(
            f'Endpoint {endpoint_dir.name} is running but has not written '
            'its connection file so it is likely still starting. Try again '
            'once it has started.',
        ) from None
    # The lock may be visible to other hosts on some file systems.
    endpoint_dir.check_not_running_elsewhere()

    logger.debug('Terminating endpoint process (PID: %s)', info.pid)
    with contextlib.suppress(ProcessLookupError):
        os.kill(info.pid, signal.SIGTERM)

    if not _wait_for_exit(info.pid, timeout=timeout):  # pragma: no cover
        logger.warning(
            'Killing endpoint process (PID: %s) which did not exit within '
            '%s seconds',
            info.pid,
            timeout,
        )
        with contextlib.suppress(ProcessLookupError):
            os.kill(info.pid, signal.SIGKILL)

    # The endpoint removes its connection file when it stops unless it was
    # killed.
    endpoint_dir.remove_connection(info)
    return True


def _status(endpoint_dir: EndpointDir) -> EndpointStatus:
    status = endpoint_dir.status()
    if status == EndpointStatus.UNKNOWN:
        # Raise the specific reason (i.e., missing or invalid config).
        endpoint_dir.check_exists()
        endpoint_dir.read_config()
    return status


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
