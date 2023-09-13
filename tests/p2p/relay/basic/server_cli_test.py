from __future__ import annotations

import asyncio
import multiprocessing
import os
import pathlib
import subprocess
import time
from unittest import mock
from unittest.mock import AsyncMock

import click
import click.testing
import pytest
import websockets

from proxystore.p2p.relay.basic.client import BasicRelayClient
from proxystore.p2p.relay.basic.server import cli
from proxystore.p2p.relay.basic.server import serve
from testing.utils import open_port


def test_invoke() -> None:
    runner = click.testing.CliRunner()
    with mock.patch('proxystore.p2p.relay.basic.server.serve', AsyncMock()):
        runner.invoke(cli)


def test_invoke_with_log_dir(tmp_path: pathlib.Path) -> None:
    tmp_dir = os.path.join(tmp_path, 'log-dir')
    assert not os.path.isdir(tmp_dir)
    runner = click.testing.CliRunner()
    with mock.patch('proxystore.p2p.relay.basic.server.serve', AsyncMock()):
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
            if 'Relay server listening on' in line:
                break

        server_handle.terminate()
        # Sleep for a small period (10ms) to allow file to get written
        time.sleep(0.01)

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
    host = '127.0.0.1'
    port = open_port()
    address = f'ws://{host}:{port}'

    process = multiprocessing.Process(target=_serve, args=(host, port))
    process.start()

    while True:
        try:
            client = BasicRelayClient(address)
            client._initial_backoff_seconds = 0.01
            await client.connect()
        except OSError:  # pragma: no cover
            await asyncio.sleep(0.01)
        else:
            # Coverage doesn't detect the singular break but it does
            # get executed to break from the loop
            break  # pragma: no cover

    pong_waiter = await client.websocket.ping()
    await asyncio.wait_for(pong_waiter, 1)

    process.terminate()

    with pytest.raises(websockets.exceptions.ConnectionClosedOK):
        await client.websocket.recv()

    process.join()

    await client.close()


@pytest.mark.timeout(5)
@pytest.mark.asyncio()
async def test_start_server_cli() -> None:
    host = '127.0.0.1'
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
        if 'Relay server listening on' in line:
            break

    client = BasicRelayClient(address)
    await client.connect()
    pong_waiter = await client.websocket.ping()
    await asyncio.wait_for(pong_waiter, 1)

    server_handle.stdout.close()
    server_handle.terminate()

    with pytest.raises(websockets.exceptions.ConnectionClosedOK):
        await client.websocket.recv()

    await client.close()
