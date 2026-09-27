"""Endpoint management commands.

These are the implementations of the commands available via the
[`proxystore-endpoint`](../cli.md#proxystore-endpoint) command.
Subsequently, all commands log errors and results and return status codes
(rather than raising errors and returning results).
"""

from __future__ import annotations

import contextlib
import logging
import os
import random
import shutil
import signal
import time
from collections.abc import Generator
from typing import Literal

import daemon.pidfile

from proxystore import utils
from proxystore.endpoint.config import DEFAULT_DATABASE_PATH
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import EndpointP2PConfig
from proxystore.endpoint.config import EndpointStorageConfig
from proxystore.endpoint.config import resolve_host
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.directory import EndpointStatus
from proxystore.endpoint.directory import is_own_process
from proxystore.endpoint.directory import resolve_home
from proxystore.endpoint.peers import PeerExistsError
from proxystore.endpoint.serve import serve

logger = logging.getLogger(__name__)


def configure_endpoint(
    name: str,
    *,
    host: str = 'ip',
    peering: bool = True,
    persist_data: bool = False,
    relays: str = 'n0',
    port: int | None,
    proxystore_dir: str | None = None,
    tls: bool = False,
) -> int:
    """Configure a new endpoint.

    Args:
        name: Name of endpoint.
        host: Method to resolve the hostname of the endpoint ("ip" or "fqdn")
            or a static address to use.
        peering: Enable communication with peer endpoints.
        persist_data: Persist data stored in the endpoint.
        relays: Relays used for peering. One of `"n0"`, `"none"`, or a
            comma-separated list of relay URLs.
        port: Port for endpoint to listen on. If `None`, a random port is
            selected.
        proxystore_dir: Optionally specify the proxystore home directory.
            Defaults to [`home_dir()`][proxystore.utils.environment.home_dir].
        tls: Encrypt connections between clients and the endpoint with TLS.

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages \
        are logged to the default logger.
    """
    database_path = DEFAULT_DATABASE_PATH if persist_data else None

    if host.lower().strip() in ('ip', 'fqdn'):
        host = host.lower().strip()

    port = port if port is not None else random.randint(10 * 1024, 20 * 1024)

    try:
        endpoint_dir = EndpointDir.create(
            name,
            proxystore_dir,
            host=host,
            port=port,
            tls=tls,
            p2p=EndpointP2PConfig(
                enabled=peering,
                relays=_parse_relays(relays),
            ),
            storage=EndpointStorageConfig(database_path=database_path),
        )
    except FileExistsError:
        logger.error('An endpoint named %s already exists.', name)
        logger.info('To reconfigure the endpoint, remove and try again.')
        return 1
    except ValueError as e:
        logger.error(str(e))
        return 1
    cfg = endpoint_dir.read_config()

    logger.info('Configured endpoint: %s <%s>', cfg.name, cfg.id)
    logger.info('Config and log file directory: %s', endpoint_dir)
    logger.info('Start the endpoint with:')
    logger.info('  $ proxystore-endpoint start %s', cfg.name)
    if peering:
        logger.info('Allow a peer endpoint to communicate with this one with:')
        logger.info(
            '  $ proxystore-endpoint peers add %s PEER_NAME PEER_ID',
            cfg.name,
        )

    return 0


def _parse_relays(relays: str) -> Literal['n0', 'none'] | list[str]:
    relays = relays.strip()
    if relays in ('n0', 'none'):
        return relays  # type: ignore[return-value]
    return [url.strip() for url in relays.split(',') if url.strip()]


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
    proxystore_dir = resolve_home(proxystore_dir)

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

    eps = [(e.name, e.id) for e in endpoints]
    eps = sorted(eps, key=lambda x: x[0])
    logger.info(
        '%-*s %-*s ID',
        max_endpoint_chars,
        'NAME',
        max_status_chars,
        'STATUS',
        extra={'simple': True},
    )

    toprule_len = 2 + max_endpoint_chars + max_status_chars + len(eps[0][1])
    logger.info('=' * toprule_len, extra={'simple': True})

    for name, endpoint_id in eps:
        status = EndpointDir.from_name(name, proxystore_dir).status()
        logger.info(
            '%-*.*s %-*.*s %s',
            max_endpoint_chars,
            max_endpoint_chars,
            name,
            max_status_chars,
            max_status_chars,
            status.name,
            endpoint_id,
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
    endpoint_dir = EndpointDir.from_name(name, proxystore_dir)

    if not os.path.exists(endpoint_dir):
        logger.error('An endpoint named %s does not exist.', name)
        return 1

    status = EndpointDir.from_name(name, proxystore_dir).status()
    if status in (EndpointStatus.RUNNING, EndpointStatus.HANGING):
        logger.error('Endpoint must be stopped before removing.')
        logger.error('  $ proxystore-endpoint stop %s', name)
        return 1

    shutil.rmtree(endpoint_dir)

    logger.info('Removed endpoint named %s.', name)

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
        name: Name of endpoint to start.
        detach: Start the endpoint as a daemon process.
        log_level: Logging level of the endpoint.
        proxystore_dir: Optionally specify the proxystore home directory.
            Defaults to [`home_dir()`][proxystore.utils.environment.home_dir].

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages \
        are logged to the default logger.
    """
    status = EndpointDir.from_name(name, proxystore_dir).status()
    if status == EndpointStatus.RUNNING:
        logger.error('Endpoint %s is already running.', name)
        return 1
    if status == EndpointStatus.UNKNOWN:
        logger.error('A valid endpoint named %s does not exist.', name)
        logger.error('Use `list` to see available endpoints.')
        return 1

    endpoint_dir = EndpointDir.from_name(name, proxystore_dir)
    pid_file = endpoint_dir.pid_path

    # Resolve the host before daemonizing so errors are shown to the user
    # rather than only written to the log.
    host = endpoint_dir.read_config().host
    try:
        resolve_host(host)
    except OSError as e:
        logger.error('Unable to resolve the host address (%s): %s', host, e)
        return 1

    if status == EndpointStatus.HANGING:
        if _running_elsewhere(endpoint_dir):
            return 1
        logger.debug('Removing invalid PID file (%s).', pid_file)
        os.remove(pid_file)

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
    status = EndpointDir.from_name(name, proxystore_dir).status()
    if status == EndpointStatus.UNKNOWN:
        logger.error('A valid endpoint named %s does not exist.', name)
        logger.error('Use `list` to see available endpoints.')
        return 1
    if status == EndpointStatus.STOPPED:
        logger.info('Endpoint %s is not running.', name)
        return 0

    endpoint_dir = EndpointDir.from_name(name, proxystore_dir)
    pid_file = endpoint_dir.pid_path

    if status == EndpointStatus.HANGING and _running_elsewhere(endpoint_dir):
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


def _running_elsewhere(endpoint_dir: EndpointDir) -> bool:
    """Check if the endpoint appears to be running on a different machine.

    The PID file only identifies a process on the machine that wrote it, so
    the connection file of the running endpoint is used to find the machine.
    An error is logged if the endpoint is running elsewhere.
    """
    try:
        info = endpoint_dir.read_connection()
    except (FileNotFoundError, ValueError):
        return False
    if info.hostname == utils.hostname():
        return False
    logger.error(
        'Endpoint %s appears to be running on %s (PID %s). Stop the '
        'endpoint on %s. If it is not running, delete the PID file at %s '
        'and try again.',
        os.path.basename(endpoint_dir.path),
        info.hostname,
        info.pid,
        info.hostname,
        endpoint_dir.pid_path,
    )
    return True


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


def _read_endpoint(
    name: str,
    proxystore_dir: str | None,
) -> tuple[EndpointDir, EndpointConfig] | None:
    endpoint_dir = EndpointDir.from_name(name, proxystore_dir)
    if not os.path.exists(endpoint_dir):
        logger.error('An endpoint named %s does not exist.', name)
        return None
    try:
        config = endpoint_dir.read_config()
    except (FileNotFoundError, ValueError) as e:
        logger.error(str(e))
        return None
    return endpoint_dir, config


def get_endpoint_id(
    name: str,
    *,
    proxystore_dir: str | None = None,
) -> int:
    """Print the ID of an endpoint.

    Share the ID with the owners of other endpoints so they can add this
    endpoint to their allowlist of peers.

    Args:
        name: Name of the endpoint.
        proxystore_dir: Optionally specify the proxystore home directory.
            Defaults to [`home_dir()`][proxystore.utils.environment.home_dir].

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages \
        are logged to the default logger.
    """
    endpoint = _read_endpoint(name, proxystore_dir)
    if endpoint is None:
        return 1
    _, config = endpoint
    logger.info(config.id, extra={'simple': True})
    return 0


def add_peer(
    name: str,
    peer_name: str,
    peer_id: str,
    *,
    proxystore_dir: str | None = None,
) -> int:
    """Add a peer endpoint to the allowlist of an endpoint.

    Args:
        name: Name of the endpoint.
        peer_name: Name to give the peer in the allowlist.
        peer_id: ID of the peer endpoint.
        proxystore_dir: Optionally specify the proxystore home directory.
            Defaults to [`home_dir()`][proxystore.utils.environment.home_dir].

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages \
        are logged to the default logger.
    """
    endpoint = _read_endpoint(name, proxystore_dir)
    if endpoint is None:
        return 1
    endpoint_dir, config = endpoint

    try:
        endpoint_id = endpoint_dir.peers.add(peer_name, peer_id)
    except PeerExistsError as e:
        logger.error('%s Remove it first with:', e)
        logger.error(
            '  $ proxystore-endpoint peers remove %s %s', name, peer_name
        )
        return 1
    except ValueError as e:
        logger.error(str(e))
        return 1

    logger.info(
        'Added peer %s <%s> to endpoint %s.', peer_name, endpoint_id, name
    )
    logger.info(
        'The peer must also add this endpoint <%s> to its peers.',
        config.id,
    )
    return 0


def remove_peer(
    name: str,
    peer_name: str,
    *,
    proxystore_dir: str | None = None,
) -> int:
    """Remove a peer endpoint from the allowlist of an endpoint.

    If the endpoint is running, the peer is denied access immediately.

    Args:
        name: Name of the endpoint.
        peer_name: Name of the peer in the allowlist.
        proxystore_dir: Optionally specify the proxystore home directory.
            Defaults to [`home_dir()`][proxystore.utils.environment.home_dir].

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages \
        are logged to the default logger.
    """
    endpoint = _read_endpoint(name, proxystore_dir)
    if endpoint is None:
        return 1
    endpoint_dir, _ = endpoint

    try:
        endpoint_id = endpoint_dir.peers.remove(peer_name)
    except ValueError as e:
        logger.error('Endpoint %s: %s', name, e)
        return 1

    logger.info(
        'Removed peer %s <%s> from endpoint %s.',
        peer_name,
        endpoint_id,
        name,
    )
    return 0


def list_peers(
    name: str,
    *,
    proxystore_dir: str | None = None,
) -> int:
    """List the peers in the allowlist of an endpoint.

    Args:
        name: Name of the endpoint.
        proxystore_dir: Optionally specify the proxystore home directory.
            Defaults to [`home_dir()`][proxystore.utils.environment.home_dir].

    Returns:
        Exit code where 0 is success and 1 is failure. Failure messages \
        are logged to the default logger.
    """
    endpoint = _read_endpoint(name, proxystore_dir)
    if endpoint is None:
        return 1
    endpoint_dir, _ = endpoint

    try:
        peers = endpoint_dir.peers.read()
    except ValueError as e:
        logger.error(str(e))
        return 1

    if len(peers.peers) == 0:
        logger.info('Endpoint %s has no peers.', name)
        logger.info('Add a peer with:')
        logger.info('  $ proxystore-endpoint peers add %s NAME ID', name)
        return 0

    max_name_chars = max(len('NAME'), *(len(n) for n in peers.peers))
    logger.info('%-*s ID', max_name_chars, 'NAME', extra={'simple': True})
    logger.info('=' * (max_name_chars + 65), extra={'simple': True})
    for peer_name, endpoint_id in sorted(peers.peers.items()):
        logger.info(
            '%-*s %s',
            max_name_chars,
            peer_name,
            endpoint_id,
            extra={'simple': True},
        )
    return 0
