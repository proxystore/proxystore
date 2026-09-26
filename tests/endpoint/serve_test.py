from __future__ import annotations

import asyncio
import multiprocessing
import os
import pathlib
import ssl
import stat
import uuid
from typing import Any
from unittest import mock
from unittest.mock import AsyncMock

import pytest
from globus_sdk.token_storage import TokenValidationError

from proxystore.endpoint.client import EndpointClient
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import EndpointStorageConfig
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.exceptions import EndpointConnectionError
from proxystore.endpoint.serve import _get_auth_headers
from proxystore.endpoint.serve import running_endpoint
from proxystore.endpoint.serve import serve
from testing.endpoint import terminate_process
from testing.endpoint import wait_for_endpoint
from testing.mocked.globus import get_testing_app
from testing.utils import open_port


def _endpoint_dir(
    path: pathlib.Path,
    **kwargs: Any,
) -> tuple[EndpointDir, EndpointConfig]:
    options: dict[str, Any] = {
        'name': 'my-endpoint',
        'uuid': str(uuid.uuid4()),
        'host': '127.0.0.1',
        'port': open_port(),
        'storage': EndpointStorageConfig(database_path=':memory:'),
    }
    options.update(kwargs)
    config = EndpointConfig(**options)
    endpoint_dir = EndpointDir(str(path))
    endpoint_dir.write_config(config)
    return endpoint_dir, config


async def test_running_endpoint(tmp_path: pathlib.Path) -> None:
    endpoint_dir, config = _endpoint_dir(tmp_path)
    connection_file = endpoint_dir.connection_path

    async with running_endpoint(endpoint_dir) as endpoint:
        assert endpoint.uuid == uuid.UUID(config.uuid)
        assert stat.S_IMODE(os.stat(connection_file).st_mode) == 0o600
        client = await asyncio.to_thread(EndpointClient.from_dir, endpoint_dir)
        assert client.info.uuid == endpoint.uuid

    # Open connections are closed and the connection file is removed on
    # shutdown
    with pytest.raises(EndpointConnectionError):
        await asyncio.to_thread(client.exists, 'key')
    assert not os.path.exists(connection_file)


async def test_running_endpoint_restricts_endpoint_dir(
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    endpoint_dir, _ = _endpoint_dir(tmp_path)
    os.chmod(tmp_path, 0o777)
    async with running_endpoint(endpoint_dir):
        pass
    assert stat.S_IMODE(os.stat(tmp_path).st_mode) == 0o700
    assert any('other permissions' in r.message for r in caplog.records)


async def test_running_endpoint_port_in_use(tmp_path: pathlib.Path) -> None:
    endpoint_dir, _ = _endpoint_dir(tmp_path)
    async with running_endpoint(endpoint_dir):
        running = endpoint_dir.read_connection()
        # A second instance fails to start without replacing or removing
        # the connection file of the running instance
        with pytest.raises(OSError):
            async with running_endpoint(endpoint_dir):
                pass  # pragma: no cover
        assert endpoint_dir.read_connection() == running
        client = await asyncio.to_thread(EndpointClient.from_dir, endpoint_dir)
        await asyncio.to_thread(client.close)


async def test_running_endpoint_start_up_failure_cleans_up(
    tmp_path: pathlib.Path,
) -> None:
    endpoint_dir, _ = _endpoint_dir(tmp_path)
    # The connection file cannot be written if its path is a directory
    os.mkdir(endpoint_dir.connection_path)
    with mock.patch.object(Endpoint, 'close', AsyncMock()) as mock_close:
        with pytest.raises(IsADirectoryError):
            async with running_endpoint(endpoint_dir):
                pass  # pragma: no cover
    mock_close.assert_awaited_once()


async def test_running_endpoint_not_started(tmp_path: pathlib.Path) -> None:
    endpoint_dir, _ = _endpoint_dir(tmp_path, host=None)
    with pytest.raises(ValueError, match='host'):
        async with running_endpoint(endpoint_dir):
            pass  # pragma: no cover


@pytest.mark.timeout(10)
def test_serve(use_uvloop: bool, tmp_path: pathlib.Path) -> None:
    endpoint_dir, config = _endpoint_dir(tmp_path)

    context = multiprocessing.get_context('spawn')
    process = context.Process(
        target=serve,
        args=(endpoint_dir,),
        kwargs={'use_uvloop': use_uvloop},
    )
    process.start()

    try:
        assert config.host is not None
        wait_for_endpoint(config.host, config.port)
        with EndpointClient.from_dir(endpoint_dir) as client:
            client.set('key', b'value')
            assert client.get('key') == b'value'

        # SIGTERM should cleanly shutdown the endpoint
        process.terminate()
        process.join(timeout=5)
        assert process.exitcode == 0
        assert not os.path.exists(endpoint_dir.connection_path)
    finally:
        terminate_process(process)


def test_serve_missing_config(
    use_uvloop: bool,
    tmp_path: pathlib.Path,
) -> None:
    with pytest.raises(FileNotFoundError):
        serve(EndpointDir(str(tmp_path)), use_uvloop=use_uvloop)


def test_serve_logging(use_uvloop: bool, tmp_path: pathlib.Path) -> None:
    # Make a sub dir that should not exist to check serve makes the dir
    tmp_dir = os.path.join(tmp_path, 'log-dir')

    def _serve(log_file: str) -> None:
        with mock.patch(
            'proxystore.endpoint.serve._serve_async',
            AsyncMock(),
        ):
            serve(
                EndpointDir(str(tmp_path)),
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


async def test_running_endpoint_cancels_nat_check(
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

    endpoint_dir, config = _endpoint_dir(tmp_path)
    config.relay.address = relay_server.address
    endpoint_dir.write_config(config)

    with mock.patch(
        'proxystore.endpoint.serve.check_nat_and_log',
        side_effect=never_finishes,
    ):
        async with running_endpoint(endpoint_dir):
            pass

    assert cancelled.is_set()


async def test_running_endpoint_tls(tmp_path: pathlib.Path) -> None:
    endpoint_dir, config = _endpoint_dir(tmp_path, tls=True)

    async with running_endpoint(endpoint_dir):
        assert endpoint_dir.read_connection().tls_fingerprint is not None
        # The TLS certificate and key are not written to the directory
        assert not any('tls' in f for f in os.listdir(endpoint_dir.path))
        client = await asyncio.to_thread(EndpointClient.from_dir, endpoint_dir)
        assert isinstance(client._socket, ssl.SSLSocket)
        assert client.info.uuid == uuid.UUID(config.uuid)
        await asyncio.to_thread(client.close)
