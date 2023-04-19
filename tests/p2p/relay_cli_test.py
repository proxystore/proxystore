from __future__ import annotations

import asyncio
import multiprocessing
import os
import pathlib
import subprocess
import sys
from unittest import mock

import click
import click.testing
import pytest
import websockets

from proxystore.p2p.relay import cli
from proxystore.p2p.relay import serve
from proxystore.p2p.relay_client import RelayServerClient
from testing.utils import open_port

if sys.version_info >= (3, 8):  # pragma: >=3.8 cover
    from unittest.mock import AsyncMock
else:  # pragma: <3.8 cover
    from asynctest import CoroutineMock as AsyncMock


def test_invoke() -> None:
    runner = click.testing.CliRunner()
    with mock.patch('proxystore.p2p.relay.serve', AsyncMock()):
        runner.invoke(cli)


def test_invoke_with_log_dir(tmp_path: pathlib.Path) -> None:
    tmp_dir = os.path.join(tmp_path, 'log-dir')
    assert not os.path.isdir(tmp_dir)
    runner = click.testing.CliRunner()
    with mock.patch('proxystore.p2p.relay.serve', AsyncMock()):
        runner.invoke(cli, ['--log-dir', str(tmp_dir)])
    assert os.path.isdir(tmp_dir)


def test_logging_config(tmp_path: pathlib.Path) -> None:
    with subprocess.Popen(
        [
            'proxystore-relay',
            '--port',
            str(open_port()),
            '--log-dir',
            str(tmp_path),
            '--log-level',
            'INFO',
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    ) as server_handle:
        # Wait for server to log that it is listening
        assert server_handle.stdout is not None
        for line in server_handle.stdout:  # pragma: no cover
            if 'Serving' in line:
                break

        server_handle.terminate()

    logs = [
        f
        for f in os.listdir(tmp_path)
        if os.path.isfile(os.path.join(tmp_path, f))
    ]
    for log in logs:
        with open(os.path.join(tmp_path, log)) as f:
            assert 'DEBUG' not in f.read()
    assert len(logs) >= 1


def _serve(host: str, port: int) -> None:
    asyncio.run(serve(host, port))


@pytest.mark.timeout(5)
@pytest.mark.asyncio()
async def test_server_without_ssl() -> None:
    host = 'localhost'
    port = open_port()
    address = f'ws://{host}:{port}'

    process = multiprocessing.Process(target=_serve, args=(host, port))
    process.start()

    while True:
        try:
            client = RelayServerClient(address)
            client.initial_backoff_seconds = 0.01
            websocket = await client.connect()
        except OSError:  # pragma: no cover
            await asyncio.sleep(0.01)
        else:
            # Coverage doesn't detect the singular break but it does
            # get executed to break from the loop
            break  # pragma: no cover

    pong_waiter = await websocket.ping()
    await asyncio.wait_for(pong_waiter, 1)

    process.terminate()

    with pytest.raises(websockets.exceptions.ConnectionClosedOK):
        await websocket.recv()

    process.join()


@pytest.mark.timeout(5)
@pytest.mark.asyncio()
async def test_start_server_cli() -> None:
    host = 'localhost'
    port = str(open_port())
    address = f'ws://{host}:{port}'

    server_handle = subprocess.Popen(
        ['proxystore-relay', '--host', host, '--port', port],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )

    # Wait for server to log that it is listening
    assert server_handle.stdout is not None
    for line in server_handle.stdout:  # pragma: no cover
        if 'Serving relay server' in line:
            break

    client = RelayServerClient(address)
    websocket = await client.connect()
    pong_waiter = await websocket.ping()
    await asyncio.wait_for(pong_waiter, 1)

    server_handle.stdout.close()
    server_handle.terminate()

    with pytest.raises(websockets.exceptions.ConnectionClosedOK):
        await websocket.recv()
