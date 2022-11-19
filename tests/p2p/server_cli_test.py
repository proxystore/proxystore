from __future__ import annotations

import asyncio
import multiprocessing
import os
import pathlib
import subprocess
import sys
from unittest import mock

import pytest
import websockets

from proxystore.p2p.client import connect
from proxystore.p2p.server import main
from testing.utils import open_port

if sys.version_info >= (3, 8):  # pragma: >=3.8 cover
    from unittest.mock import AsyncMock
else:  # pragma: <3.8 cover
    from asynctest import CoroutineMock as AsyncMock


def test_logging_dir(tmp_path: pathlib.Path) -> None:
    tmp_dir = os.path.join(tmp_path, 'log-dir')
    assert not os.path.isdir(tmp_dir)
    with mock.patch('proxystore.p2p.server.serve', AsyncMock()):
        main(['--log-dir', str(tmp_dir)])
    assert os.path.isdir(tmp_dir)


def test_logging_config(tmp_path: pathlib.Path) -> None:
    with subprocess.Popen(
        [
            'signaling-server',
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
            if 'serving' in line:
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


@pytest.mark.timeout(5)
@pytest.mark.asyncio
async def test_server_with_mock_ssl() -> None:
    host = 'localhost'
    port = open_port()
    address = f'ws://{host}:{port}'

    process = multiprocessing.Process(
        target=main,
        args=(
            ['--host', host, '--port', str(port), '--log-level', 'CRITICAL'],
        ),
    )
    process.start()

    while True:
        try:
            _, _, websocket = await connect(address)
        except OSError:
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


@pytest.mark.timeout(5)
@pytest.mark.asyncio
async def test_start_server_cli() -> None:
    host = 'localhost'
    port = str(open_port())
    address = f'ws://{host}:{port}'

    server_handle = subprocess.Popen(
        ['signaling-server', '--host', host, '--port', port],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )

    # Wait for server to log that it is listening
    assert server_handle.stdout is not None
    for line in server_handle.stdout:  # pragma: no cover
        if 'serving signaling server' in line:
            break

    _, _, websocket = await connect(address)
    pong_waiter = await websocket.ping()
    await asyncio.wait_for(pong_waiter, 1)

    server_handle.stdout.close()
    server_handle.terminate()

    with pytest.raises(websockets.exceptions.ConnectionClosedOK):
        await websocket.recv()
