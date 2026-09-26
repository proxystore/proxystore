from __future__ import annotations

import asyncio
import multiprocessing
import os
import pathlib
import socket
import ssl
import stat
import uuid
from typing import Any
from unittest import mock
from unittest.mock import AsyncMock

import pytest
from globus_sdk.token_storage import TokenValidationError

from proxystore.endpoint.auth import read_token_file
from proxystore.endpoint.client import EndpointClient
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import EndpointFiles
from proxystore.endpoint.config import EndpointStorageConfig
from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.exceptions import EndpointConnectionError
from proxystore.endpoint.serve import _get_auth_headers
from proxystore.endpoint.serve import _serve_async
from proxystore.endpoint.serve import serve
from testing.endpoint import terminate_process
from testing.endpoint import wait_for_endpoint
from testing.mocked.globus import get_testing_app
from testing.utils import open_port


def _endpoint_config(**kwargs: Any) -> EndpointConfig:
    options: dict[str, Any] = {
        'name': 'my-endpoint',
        'uuid': str(uuid.uuid4()),
        'host': '127.0.0.1',
        'port': open_port(),
        'storage': EndpointStorageConfig(database_path=':memory:'),
    }
    options.update(kwargs)
    return EndpointConfig(**options)


async def test_serve_async_token_file(tmp_path: pathlib.Path) -> None:
    config = _endpoint_config()
    token_file = EndpointFiles(str(tmp_path)).token
    stop = asyncio.Event()
    task = asyncio.create_task(_serve_async(config, str(tmp_path), stop))

    for _ in range(500):  # pragma: no branch
        if os.path.exists(token_file):
            break
        await asyncio.sleep(0.01)
    assert stat.S_IMODE(os.stat(token_file).st_mode) == 0o600

    assert config.host is not None
    await asyncio.to_thread(wait_for_endpoint, config.host, config.port)
    token = read_token_file(token_file)
    client = await asyncio.to_thread(
        EndpointClient.connect,
        config.host,
        config.port,
        token,
    )
    assert client.info.uuid == uuid.UUID(config.uuid)

    stop.set()
    await task
    # Open connections are closed and the token is removed on shutdown
    with pytest.raises(EndpointConnectionError):
        await asyncio.to_thread(client.exists, 'key')
    assert not os.path.exists(token_file)


async def test_serve_async_restricts_endpoint_dir(
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    os.chmod(tmp_path, 0o777)
    stop = asyncio.Event()
    stop.set()
    await _serve_async(_endpoint_config(), str(tmp_path), stop)
    assert stat.S_IMODE(os.stat(tmp_path).st_mode) == 0o755
    assert any('write permissions' in r.message for r in caplog.records)


async def test_serve_async_port_in_use(tmp_path: pathlib.Path) -> None:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        sock.listen()
        config = _endpoint_config(port=sock.getsockname()[1])
        with pytest.raises(OSError):
            await _serve_async(config, str(tmp_path))
    assert not os.path.exists(EndpointFiles(str(tmp_path)).token)


async def test_serve_async_start_up_failure_cleans_up(
    tmp_path: pathlib.Path,
) -> None:
    config = _endpoint_config()
    # The token cannot be written to a directory that does not exist
    with mock.patch.object(Endpoint, 'close', AsyncMock()) as mock_close:
        with pytest.raises(FileNotFoundError):
            await _serve_async(config, str(tmp_path / 'missing'))
    mock_close.assert_awaited_once()


@pytest.mark.timeout(10)
def test_serve(use_uvloop: bool, tmp_path: pathlib.Path) -> None:
    config = _endpoint_config()
    endpoint_dir = str(tmp_path)

    context = multiprocessing.get_context('spawn')
    process = context.Process(
        target=serve,
        args=(config,),
        kwargs={'endpoint_dir': endpoint_dir, 'use_uvloop': use_uvloop},
    )
    process.start()

    try:
        assert config.host is not None
        wait_for_endpoint(config.host, config.port)
        with EndpointClient.from_config(config, endpoint_dir) as client:
            client.set('key', b'value')
            assert client.get('key') == b'value'

        # SIGTERM should cleanly shutdown the endpoint
        process.terminate()
        process.join(timeout=5)
        assert process.exitcode == 0
        assert not os.path.exists(EndpointFiles(endpoint_dir).token)
    finally:
        terminate_process(process)


def test_serve_config_validation(
    use_uvloop: bool,
    tmp_path: pathlib.Path,
) -> None:
    config = _endpoint_config(host=None)
    with pytest.raises(ValueError, match='host'):
        serve(config, endpoint_dir=str(tmp_path), use_uvloop=use_uvloop)


def test_serve_logging(use_uvloop: bool, tmp_path: pathlib.Path) -> None:
    # Make a sub dir that should not exist to check serve makes the dir
    tmp_dir = os.path.join(tmp_path, 'log-dir')

    def _serve(log_file: str) -> None:
        with mock.patch(
            'proxystore.endpoint.serve._serve_async',
            AsyncMock(),
        ):
            serve(
                _endpoint_config(),
                endpoint_dir=str(tmp_path),
                log_level='INFO',
                log_file=log_file,
                use_uvloop=use_uvloop,
            )

    # Make directory if necessary
    log_file = os.path.join(tmp_dir, 'log.txt')
    _serve(log_file)
    assert os.path.isdir(tmp_dir)
    assert os.path.exists(log_file)

    # Write log to existing log directory
    log_file2 = os.path.join(tmp_dir, 'log2.txt')
    _serve(log_file2)
    assert os.path.isdir(tmp_dir)
    assert os.path.exists(log_file2)


def test_get_auth_headers_none() -> None:
    assert _get_auth_headers(None) == {}


def test_get_auth_headers_globus() -> None:
    globus_app = get_testing_app()
    mock_authorizer = mock.MagicMock()
    header = 'Bearer <TOKEN>'

    with (
        mock.patch(
            'proxystore.endpoint.serve.get_globus_app',
            return_value=globus_app,
        ),
        mock.patch.object(
            globus_app,
            'get_authorizer',
            return_value=mock_authorizer,
        ),
        mock.patch.object(
            mock_authorizer,
            'get_authorization_header',
            return_value=header,
        ),
    ):
        assert _get_auth_headers('globus')['Authorization'] == header


def test_get_auth_headers_globus_missing() -> None:
    globus_app = get_testing_app()

    with (
        mock.patch(
            'proxystore.endpoint.serve.get_globus_app',
            return_value=globus_app,
        ),
        mock.patch.object(
            globus_app,
            'get_authorizer',
            side_effect=TokenValidationError(),
        ),
        pytest.raises(
            SystemExit,
        ),
    ):
        assert _get_auth_headers('globus')


async def test_serve_cancels_nat_check(
    relay_server,
    tmp_path: pathlib.Path,
) -> None:
    # The NAT check runs concurrently with serving so that a slow or blocked
    # network cannot delay the endpoint from accepting requests. Shutting the
    # endpoint down must therefore cancel a check which has not finished
    # rather than wait for it.
    cancelled = asyncio.Event()

    async def never_finishes() -> None:
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    config = _endpoint_config()
    config.relay.address = relay_server.address

    stop = asyncio.Event()
    stop.set()
    with mock.patch(
        'proxystore.endpoint.serve.check_nat_and_log',
        side_effect=never_finishes,
    ):
        await _serve_async(config, str(tmp_path), stop)

    assert cancelled.is_set()


async def test_serve_async_tls(tmp_path: pathlib.Path) -> None:
    config = _endpoint_config(tls=True)
    endpoint_dir = str(tmp_path)
    cert_file = EndpointFiles(endpoint_dir).tls_cert
    key_file = EndpointFiles(endpoint_dir).tls_key
    stop = asyncio.Event()
    task = asyncio.create_task(_serve_async(config, endpoint_dir, stop))

    assert config.host is not None
    await asyncio.to_thread(wait_for_endpoint, config.host, config.port)
    assert stat.S_IMODE(os.stat(key_file).st_mode) == 0o600

    client = await asyncio.to_thread(
        EndpointClient.from_config,
        config,
        endpoint_dir,
    )
    assert isinstance(client._socket, ssl.SSLSocket)
    assert client.info.uuid == uuid.UUID(config.uuid)
    await asyncio.to_thread(client.close)

    stop.set()
    await task
    assert not os.path.exists(cert_file)
    assert not os.path.exists(key_file)
