from __future__ import annotations

import contextlib
import logging
import multiprocessing
import subprocess
import time

import pytest
import requests

from proxystore.endpoint.serve import main


@pytest.mark.timeout(5)
def test_endpoint() -> None:
    def run_without_stdout(args: tuple[str]) -> None:
        with contextlib.redirect_stdout(None), contextlib.redirect_stderr(
            None,
        ):
            logging.disable(10000)
            main(args)

    host, port = 'localhost', 5823
    process = multiprocessing.Process(
        target=run_without_stdout,
        args=(('--host', host, '--port', str(port)),),
    )
    process.start()

    try:
        while True:
            try:
                r = requests.get(f'http://{host}:{port}/')
            except requests.exceptions.ConnectionError:
                time.sleep(0.1)
                continue
            if r.status_code == 200:  # pragma: no branch
                break
    finally:
        process.terminate()


@pytest.mark.timeout(5)
def test_start_server_cli() -> None:
    host, port = 'localhost', 5824
    server_handle = subprocess.Popen(
        ['endpoint-start', '--host', host, '--port', str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        while True:
            try:
                r = requests.get(f'http://{host}:{port}/')
            except requests.exceptions.ConnectionError:
                time.sleep(0.1)
                continue
            if r.status_code == 200:  # pragma: no branch
                break
    finally:
        server_handle.terminate()
