from __future__ import annotations

import asyncio
import logging
import multiprocessing
import os
import pathlib
import subprocess
import time
import uuid
from unittest import mock
from unittest.mock import AsyncMock

import click
import click.testing
import pytest
import websockets

from proxystore.p2p.relay.authenticate import NullAuthenticator
from proxystore.p2p.relay.authenticate import NullUser
from proxystore.p2p.relay.client import RelayClient
from proxystore.p2p.relay.config import RelayServingConfig
from proxystore.p2p.relay.manager import Client
from proxystore.p2p.relay.run import cli
from proxystore.p2p.relay.run import periodic_client_logger
from proxystore.p2p.relay.run import serve
from proxystore.p2p.relay.server import RelayServer
from testing.ssl import SSLContextFixture
from testing.utils import open_port


@pytest.mark.asyncio()
async def test_periodic_client_logger(caplog) -> None:
    caplog.set_level(logging.INFO)

    server = RelayServer(NullAuthenticator())
    client = Client(
        name='test',
        uuid=uuid.uuid4(),
        user=NullUser(),
        websocket=mock.MagicMock(),
    )
    server.client_manager.add_client(client)

    task = periodic_client_logger(server, 0.001)
    await asyncio.sleep(0.01)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert any(
        [
            'Connected clients: 1' in record.message
            and record.levelname == 'INFO'
            for record in caplog.records
        ],
    )
    assert any(
        [
            str(client.uuid) in record.message and record.levelname == 'INFO'
            for record in caplog.records
        ],
    )


def test_invoke() -> None:
    runner = click.testing.CliRunner()
    with mock.patch(
        'proxystore.p2p.relay.run.serve',
        AsyncMock(),
    ) as mock_serve:
        runner.invoke(cli)
        mock_serve.assert_awaited_once()


def test_invoke_and_override_defaults(tmp_path: pathlib.Path) -> None:
    tmp_dir = os.path.join(tmp_path, 'log-dir')
    assert not os.path.isdir(tmp_dir)

    async def _mock_serve(config: RelayServingConfig) -> None:
        assert config.host == 'test-host'
        assert config.port == 1234
        assert config.logging.log_dir == str(tmp_dir)
        assert config.logging.default_level == logging.WARNING

    options: list[str] = []
    options += ['--host', 'test-host']
    options += ['--port', '1234']
    options += ['--log-dir', str(tmp_dir)]
    options += ['--log-level', 'WARNING']

    runner = click.testing.CliRunner()
    with mock.patch(
        'proxystore.p2p.relay.run.serve',
        AsyncMock(side_effect=_mock_serve),
    ):
        runner.invoke(cli, options)

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
        server_handle.wait(1)

    sleep_time = 0.01
    max_wait_time = 1.0
    waited_time = 0.0
    while waited_time <= max_wait_time:  # pragma: no branch
        logs = [
            f
            for f in os.listdir(tmp_path)
            if os.path.isfile(os.path.join(tmp_path, f))
        ]
        if len(logs) >= 1:
            for log in logs:
                with open(os.path.join(tmp_path, log)) as f:
                    assert 'DEBUG' not in f.read()
            break
        elif waited_time >= max_wait_time:  # pragma: no cover
            raise TimeoutError('Timeout waiting for log file to be written.')
        else:  # pragma: no cover
            time.sleep(sleep_time)
            waited_time += sleep_time


def _serve(config: RelayServingConfig) -> None:
    asyncio.run(serve(config))


@pytest.mark.parametrize('use_ssl', (True, False))
@pytest.mark.timeout(5)
@pytest.mark.asyncio()
async def test_serve_in_subprocess(
    use_ssl: bool,
    ssl_context: SSLContextFixture,
) -> None:
    config = RelayServingConfig(host='127.0.0.1', port=open_port())

    if use_ssl:
        config.certfile = ssl_context.certfile
        config.keyfile = ssl_context.keyfile

    prefix = 'wss://' if use_ssl else 'ws://'
    address = f'{prefix}{config.host}:{config.port}'

    process = multiprocessing.Process(target=_serve, args=(config,))
    process.start()

    while True:
        try:
            client = RelayClient(
                address,
                reconnect_task=False,
                verify_certificate=False,
            )
            await client.connect(retry=False)
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
