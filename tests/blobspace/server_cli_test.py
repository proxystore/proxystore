from __future__ import annotations

import asyncio
import multiprocessing
import subprocess

import pytest
import websockets

from proxystore.blobspace.server import connect
from proxystore.blobspace.server import main


SERVER_HOST = 'localhost'
SERVER_PORT = 8765
SERVER_ADDRESS = f'{SERVER_HOST}:{SERVER_PORT}'
SERVER_ARGS = (
    '--host',
    SERVER_HOST,
    '--port',
    str(SERVER_PORT),
)


@pytest.mark.timeout(5)
@pytest.mark.asyncio
async def test_server() -> None:
    process = multiprocessing.Process(target=main, args=(SERVER_ARGS,))
    process.start()

    while True:
        try:
            # print('here')
            websocket = await connect(
                uuid='fake-uuid',
                name='test-client',
                address=SERVER_ADDRESS,
            )
        except ConnectionRefusedError:
            await asyncio.sleep(0.1)
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
    server_handle = subprocess.Popen(
        ['signaling-server-start', *SERVER_ARGS],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )

    # Wait for server to log that it is listening
    for line in server_handle.stdout:  # pragma: no branch
        if 'listening on' in line:
            break

    websocket = await connect(
        uuid='fake-uuid',
        name='test-client',
        address=SERVER_ADDRESS,
    )
    pong_waiter = await websocket.ping()
    await asyncio.wait_for(pong_waiter, 1)

    server_handle.terminate()

    with pytest.raises(websockets.exceptions.ConnectionClosedOK):
        await websocket.recv()
