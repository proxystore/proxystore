"""Utilities for launching endpoints in tests."""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from multiprocessing import Process

import requests

from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.serve import serve


def serve_endpoint_silent(
    name: str,
    uuid: uuid.UUID,
    host: str,
    port: int,
    server: str | None,
) -> None:
    """Serve endpoint and suppress all output."""
    with contextlib.redirect_stdout(None), contextlib.redirect_stderr(
        None,
    ):
        logging.disable(100000)
        # https://stackoverflow.com/questions/66583461
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        config = EndpointConfig(
            name=name,
            uuid=uuid,
            host=host,
            port=port,
            server=server,
        )
        serve(config)
        loop.close()


def launch_endpoint(
    name: str,
    uuid: uuid.UUID,
    host: str,
    port: int,
    server: str | None,
) -> Process:
    """Launch endpoint in subprocess."""
    server_handle = Process(
        target=serve_endpoint_silent,
        args=(name, uuid, host, port, server),
    )
    server_handle.start()

    while True:
        try:
            r = requests.get(f'http://{host}:{port}/')
        except requests.exceptions.ConnectionError:
            time.sleep(0.01)
            continue
        if r.status_code == 200:  # pragma: no branch
            break

    return server_handle
