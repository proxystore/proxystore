from __future__ import annotations

import asyncio
import multiprocessing
import os
import subprocess
from unittest import mock

import pytest
import websockets

from proxystore.p2p.server import connect
from proxystore.p2p.server import main
from testing.utils import open_port


def test_logging_dir(tmp_dir) -> None:
    assert not os.path.isdir(tmp_dir)
    with mock.patch('proxystore.p2p.server.serve'):
        main(['--log-dir', tmp_dir])
    assert os.path.isdir(tmp_dir)


def test_logging_config(tmp_dir) -> None:
    server_handle = subprocess.Popen(
        [
            'signaling-server',
            '--port',
            str(open_port()),
            '--log-dir',
            tmp_dir,
            '--log-level',
            'INFO',
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )

    # Wait for server to log that it is listening
    for line in server_handle.stdout:  # pragma: no cover
        if 'serving' in line:
            break

    server_handle.terminate()

    logs = [
        f
        for f in os.listdir(tmp_dir)
        if os.path.isfile(os.path.join(tmp_dir, f))
    ]
    for log in logs:
        with open(os.path.join(tmp_dir, log)) as f:
            assert 'DEBUG' not in f.read()
    assert len(logs) >= 1


@pytest.mark.timeout(5)
@pytest.mark.asyncio
async def test_server() -> None:
    host = 'localhost'
    port = str(open_port())
    address = f'{host}:{port}'

    process = multiprocessing.Process(
        target=main,
        args=(['--host', host, '--port', port, '--log-level', 'CRITICAL'],),
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
    address = f'{host}:{port}'

    server_handle = subprocess.Popen(
        ['signaling-server', '--host', host, '--port', port],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )

    # Wait for server to log that it is listening
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
