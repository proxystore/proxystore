"""Utilities for launching endpoints in tests."""

from __future__ import annotations

import contextlib
import logging
import multiprocessing
import os
import shutil
import time
from collections.abc import Generator
from typing import Any

import pytest

from proxystore.endpoint.client import EndpointClient
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import EndpointP2PConfig
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.exceptions import EndpointError
from proxystore.endpoint.process import serve
from testing.utils import open_port


def write_endpoint(
    proxystore_dir: str,
    name: str,
    **kwargs: Any,
) -> tuple[EndpointDir, EndpointConfig]:
    """Create an endpoint for testing.

    Args:
        proxystore_dir: ProxyStore home directory to create the endpoint in.
        name: Name of the endpoint.
        kwargs: Fields of the configuration. The `port` defaults to an open
            port and peering is disabled by default.

    Returns:
        The endpoint directory and configuration.
    """
    options: dict[str, Any] = {
        'port': open_port(),
        # Peering connects to n0's public relays so it is disabled by default
        # to avoid network access outside of the host in tests.
        'p2p': EndpointP2PConfig(enabled=False),
        **kwargs,
    }
    endpoint_dir = EndpointDir.create(name, proxystore_dir, **options)
    return endpoint_dir, endpoint_dir.read_config()


def serve_endpoint_silent(
    endpoint_dir: EndpointDir,
    *,
    use_uvloop: bool = False,
) -> None:
    """Serve endpoint and suppress all output.

    Warning:
        This should be run in a subprocess.
    """
    with contextlib.redirect_stdout(None), contextlib.redirect_stderr(None):
        logging.disable(100000)
        serve(endpoint_dir, use_uvloop=use_uvloop)


def terminate_process(
    process: multiprocessing.process.BaseProcess,
) -> None:
    """Terminate a process, killing it if it does not exit promptly."""
    process.terminate()
    process.join(timeout=5)
    if process.exitcode is None:  # pragma: no cover
        process.kill()
        process.join()


def wait_for_endpoint(
    endpoint_dir: EndpointDir, max_time_s: float = 5
) -> None:
    """Wait for the endpoint in the directory to accept clients.

    The endpoint writes its connection file after it starts listening, so
    this waits until a client can connect using the connection file.
    """
    waited_s = 0.0
    sleep_s = 0.01

    while True:
        try:
            with EndpointClient.from_dir(endpoint_dir, timeout=1):
                break
        except EndpointError as e:
            if waited_s >= max_time_s:  # pragma: no cover
                raise RuntimeError(
                    'Unable to connect to endpoint within the timeout '
                    f'({max_time_s} seconds).',
                ) from e
            time.sleep(sleep_s)
            waited_s += sleep_s


def copy_endpoint_dir(
    endpoint_dir: EndpointDir,
    proxystore_dir: str,
) -> EndpointDir:
    """Copy an endpoint directory into another ProxyStore home directory.

    This copies the config and connection files so clients using
    `proxystore_dir` can connect to the endpoint.

    Returns:
        The copied endpoint directory.
    """
    dest = EndpointDir.from_name(
        os.path.basename(endpoint_dir.path),
        proxystore_dir,
    )
    shutil.copytree(endpoint_dir.path, dest.path, dirs_exist_ok=True)
    return dest


@pytest.fixture(scope='session')
def endpoint_dir(tmp_path_factory: pytest.TempPathFactory) -> EndpointDir:
    """Directory of the endpoint fixture.

    The parent of this directory can be used as a ProxyStore home directory.
    """
    home = tmp_path_factory.mktemp('endpoint-home')
    return EndpointDir.from_name('endpoint-fixture', str(home))


@pytest.fixture(scope='session')
def endpoint(
    endpoint_dir: EndpointDir,
    use_uvloop: bool,
) -> Generator[EndpointConfig, None, None]:
    """Launch endpoint in subprocess."""
    _, config = write_endpoint(
        os.path.dirname(endpoint_dir.path),
        os.path.basename(endpoint_dir.path),
        host='localhost',
    )
    context = multiprocessing.get_context('spawn')
    server_handle = context.Process(
        target=serve_endpoint_silent,
        args=[endpoint_dir],
        kwargs={'use_uvloop': use_uvloop},
    )

    try:
        server_handle.start()

        wait_for_endpoint(endpoint_dir)
    except BaseException:  # pragma: no cover
        # Setup failed so terminate the child before re-raising, otherwise
        # the orphaned non-daemon spawn process blocks interpreter exit.
        terminate_process(server_handle)
        raise

    yield config

    terminate_process(server_handle)
