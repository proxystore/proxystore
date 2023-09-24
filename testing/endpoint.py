"""Utilities for launching endpoints in tests."""
from __future__ import annotations

import contextlib
import logging
import time
import uuid
from multiprocessing import Process
from typing import Generator

import pytest
import requests

from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.serve import serve
from testing.utils import open_port


def serve_endpoint_silent(
    config: EndpointConfig,
    *,
    use_uvloop: bool = False,
) -> None:
    """Serve endpoint and suppress all output.

    Warning:
        This should be run in a subprocess.
    """
    with contextlib.redirect_stdout(None), contextlib.redirect_stderr(None):
        logging.disable(100000)
        serve(config, use_uvloop=use_uvloop)


def wait_for_endpoint(host: str, port: int, max_time_s: float = 5) -> None:
    """Wait for the endpoint at host:port to be available."""
    waited_s = 0.0
    sleep_s = 0.01

    while True:
        try:
            r = requests.get(f'http://{host}:{port}/')
        except requests.exceptions.ConnectionError as e:
            if waited_s >= max_time_s:  # pragma: no cover
                raise RuntimeError(
                    'Unable to connect to endpoint within the timeout '
                    f'({max_time_s} seconds).',
                ) from e
            time.sleep(sleep_s)
            waited_s += sleep_s
            continue
        if r.status_code == 200:  # pragma: no branch
            break


@pytest.fixture(scope='session')
def endpoint(use_uvloop: bool) -> Generator[EndpointConfig, None, None]:
    """Launch endpoint in subprocess."""
    config = EndpointConfig(
        name='endpoint-fixture',
        uuid=str(uuid.uuid4()),
        host='localhost',
        port=open_port(),
    )
    server_handle = Process(
        target=serve_endpoint_silent,
        args=[config],
        kwargs={'use_uvloop': use_uvloop},
    )
    server_handle.start()

    assert config.host is not None
    wait_for_endpoint(config.host, config.port)

    yield config

    server_handle.terminate()
    server_handle.join()
