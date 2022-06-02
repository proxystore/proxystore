"""Utilities for launching endpoints in tests."""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from multiprocessing import Process

import requests

from proxystore.endpoint.serve import serve


def serve_endpoint_silent(
    host: str,
    port: int,
    proxystore_dir: str | None,
    signaling_server: str | None,
) -> None:
    """Serve endpoint and suppress all output."""
    with contextlib.redirect_stdout(None), contextlib.redirect_stderr(
        None,
    ):
        logging.disable(100000)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        serve(
            host,
            port,
            proxystore_dir=proxystore_dir,
            signaling_server=signaling_server,
        )
        loop.close()


def launch_endpoint(
    host: str,
    port: int,
    proxystore_dir: str | None,
    signaling_server: str | None,
) -> Process:
    """Launch endpoint in subprocess."""
    server_handle = Process(
        target=serve_endpoint_silent,
        args=(host, port, proxystore_dir, signaling_server),
    )
    server_handle.start()

    while True:
        try:
            r = requests.get(f'http://{host}:{port}/')
        except requests.exceptions.ConnectionError:
            time.sleep(0.05)
            continue
        if r.status_code == 200:  # pragma: no branch
            break

    return server_handle
