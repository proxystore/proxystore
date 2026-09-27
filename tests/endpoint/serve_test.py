from __future__ import annotations

import asyncio
import logging
import multiprocessing
import os
import pathlib
import ssl
import stat
from typing import Any
from unittest import mock
from unittest.mock import AsyncMock

import iroh
import pytest

from proxystore.endpoint.client import EndpointClient
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import EndpointP2PConfig
from proxystore.endpoint.config import EndpointStorageConfig
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.exceptions import EndpointConfigError
from proxystore.endpoint.exceptions import EndpointConnectionError
from proxystore.endpoint.identity import SecretKey
from proxystore.endpoint.serve import running_endpoint
from proxystore.endpoint.serve import serve
from proxystore.p2p.manager import relay_options
from proxystore.utils.environment import hostname
from testing.endpoint import terminate_process
from testing.endpoint import wait_for_endpoint
from testing.endpoint import write_endpoint


def _endpoint_dir(
    path: pathlib.Path,
    **kwargs: Any,
) -> tuple[EndpointDir, EndpointConfig]:
    options: dict[str, Any] = {
        'host': '127.0.0.1',
        'storage': EndpointStorageConfig(database_path=':memory:'),
    }
    options.update(kwargs)
    return write_endpoint(str(path), 'my-endpoint', **options)


async def test_running_endpoint(tmp_path: pathlib.Path) -> None:
    endpoint_dir, config = _endpoint_dir(tmp_path)
    connection_file = endpoint_dir.connection_path

    async with running_endpoint(endpoint_dir) as endpoint:
        assert endpoint.id == config.id
        assert stat.S_IMODE(os.stat(connection_file).st_mode) == 0o600
        client = await asyncio.to_thread(EndpointClient.from_dir, endpoint_dir)
        assert client.info.id == endpoint.id

    # Open connections are closed and the connection file is removed on
    # shutdown
    with pytest.raises(EndpointConnectionError):
        await asyncio.to_thread(client.exists, 'key')
    assert not os.path.exists(connection_file)


@pytest.mark.parametrize(
    ('database_path', 'max_object_size', 'expected'),
    ((None, 0, None), (':memory:', 0, None), (None, 100, 100)),
)
async def test_running_endpoint_object_size_limit(
    database_path: str | None,
    max_object_size: int,
    expected: int | None,
    tmp_path: pathlib.Path,
) -> None:
    storage = EndpointStorageConfig(
        database_path=database_path,
        max_object_size=max_object_size,
    )
    endpoint_dir, _ = _endpoint_dir(tmp_path, storage=storage)

    async with running_endpoint(endpoint_dir):
        client = await asyncio.to_thread(EndpointClient.from_dir, endpoint_dir)
        assert client.info.max_object_size == expected
        # An object larger than the limit can be set if there is no limit
        data = b'x' * 1000
        if expected is None:
            await asyncio.to_thread(client.set, 'key', data)
            assert await asyncio.to_thread(client.get, 'key') == data
        client.close()


async def test_running_endpoint_restricts_endpoint_dir(
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    endpoint_dir, _ = _endpoint_dir(tmp_path)
    os.chmod(endpoint_dir.path, 0o777)
    async with running_endpoint(endpoint_dir):
        pass
    assert stat.S_IMODE(os.stat(endpoint_dir.path).st_mode) == 0o700
    assert any('other permissions' in r.message for r in caplog.records)


async def test_running_endpoint_missing_secret_key(
    tmp_path: pathlib.Path,
) -> None:
    endpoint_dir, _ = _endpoint_dir(tmp_path)
    os.remove(endpoint_dir.secret_key_path)
    with pytest.raises(EndpointConfigError, match='does not contain a secret'):
        async with running_endpoint(endpoint_dir):
            pass  # pragma: no cover


async def test_running_endpoint_secret_key_mismatch(
    tmp_path: pathlib.Path,
) -> None:
    endpoint_dir, _ = _endpoint_dir(tmp_path)
    endpoint_dir.write_secret_key(SecretKey.generate())
    with pytest.raises(ValueError, match='does not match the secret key'):
        async with running_endpoint(endpoint_dir):
            pass  # pragma: no cover
    assert not os.path.exists(endpoint_dir.connection_path)


async def test_running_endpoint_port_in_use(tmp_path: pathlib.Path) -> None:
    endpoint_dir, _ = _endpoint_dir(tmp_path)
    async with running_endpoint(endpoint_dir):
        running = endpoint_dir.read_connection()
        # A second instance fails to start without replacing or removing
        # the connection file of the running instance
        with pytest.raises(OSError, match='address already in use'):
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
    with (
        mock.patch.object(Endpoint, 'close', AsyncMock()) as mock_close,
        pytest.raises(IsADirectoryError),
    ):
        async with running_endpoint(endpoint_dir):
            pass  # pragma: no cover
    mock_close.assert_awaited_once()


@pytest.mark.parametrize(
    ('host', 'expected'),
    (('127.0.0.1', '127.0.0.1'), ('ip', '127.0.0.1'), ('fqdn', 'localhost')),
)
async def test_running_endpoint_resolves_host(
    host: str,
    expected: str,
    tmp_path: pathlib.Path,
) -> None:
    endpoint_dir, _ = _endpoint_dir(tmp_path, host=host)
    with open(endpoint_dir.config_path, 'rb') as f:
        before = f.read()
    with (
        mock.patch('socket.gethostbyname', return_value='127.0.0.1'),
        mock.patch('socket.getfqdn', return_value='localhost'),
    ):
        async with running_endpoint(endpoint_dir):
            info = endpoint_dir.read_connection()
            assert info.host == expected
            assert info.hostname == hostname()
            assert info.pid == os.getpid()
    # The configuration is never modified by the endpoint
    with open(endpoint_dir.config_path, 'rb') as f:
        assert f.read() == before


@pytest.mark.timeout(10)
def test_serve(use_uvloop: bool, tmp_path: pathlib.Path) -> None:
    endpoint_dir, _ = _endpoint_dir(tmp_path)

    context = multiprocessing.get_context('spawn')
    process = context.Process(
        target=serve,
        args=(endpoint_dir,),
        kwargs={'use_uvloop': use_uvloop},
    )
    process.start()

    try:
        wait_for_endpoint(endpoint_dir)
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


async def test_running_endpoint_tls(tmp_path: pathlib.Path) -> None:
    endpoint_dir, config = _endpoint_dir(tmp_path, tls=True)

    async with running_endpoint(endpoint_dir):
        assert endpoint_dir.read_connection().tls_fingerprint is not None
        # The TLS certificate and key are not written to the directory
        assert not any('tls' in f for f in os.listdir(endpoint_dir.path))
        client = await asyncio.to_thread(EndpointClient.from_dir, endpoint_dir)
        assert isinstance(client._socket, ssl.SSLSocket)
        assert client.info.id == config.id
        await asyncio.to_thread(client.close)


async def test_running_endpoint_peering(
    tmp_path: pathlib.Path, caplog
) -> None:
    caplog.set_level(logging.INFO)
    endpoint_dir, _ = _endpoint_dir(
        tmp_path,
        p2p=EndpointP2PConfig(enabled=True),
    )
    async with running_endpoint(endpoint_dir) as endpoint:
        assert endpoint.peer_manager is not None
        assert endpoint.peer_manager.id == endpoint.id
    assert any('Loaded 0 peer(s)' in r.message for r in caplog.records)


@pytest.mark.parametrize(
    'relays',
    ('n0', 'none', ['https://relay.example.com']),
)
def test_relay_options(relays: Any) -> None:
    preset, relay_mode = relay_options(EndpointP2PConfig(relays=relays))
    assert isinstance(preset, iroh.Preset)
    if relays == 'n0':
        assert relay_mode is None
    else:
        assert isinstance(relay_mode, iroh.RelayMode)


async def test_running_endpoint_peering_addr_cache(
    tmp_path: pathlib.Path,
) -> None:
    endpoint_dir, _ = _endpoint_dir(
        tmp_path,
        p2p=EndpointP2PConfig(enabled=True, relays='none'),
    )
    async with running_endpoint(endpoint_dir) as endpoint:
        assert endpoint.peer_manager is not None
        assert endpoint.peer_manager._addr_cache is not None
        path = endpoint.peer_manager._addr_cache.path
        assert path == endpoint_dir.peer_addrs_path


@pytest.mark.parametrize('relative', (True, False))
async def test_running_endpoint_database_path(
    relative: bool,
    tmp_path: pathlib.Path,
) -> None:
    other = tmp_path / 'lustre'
    other.mkdir()
    path = 'blobs.db' if relative else str(other / 'blobs.db')
    storage = EndpointStorageConfig(database_path=path)
    endpoint_dir, _ = _endpoint_dir(tmp_path, storage=storage)
    async with running_endpoint(endpoint_dir) as endpoint:
        await endpoint.set('key', b'value')
    expected = (
        os.path.join(endpoint_dir.path, 'blobs.db')
        if relative
        else str(other / 'blobs.db')
    )
    assert os.path.isfile(expected)
