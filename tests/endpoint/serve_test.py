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
from proxystore.endpoint.exceptions import EndpointConnectionError
from proxystore.endpoint.identity import SecretKey
from proxystore.endpoint.p2p.manager import relay_options
from proxystore.endpoint.serve import EndpointService
from proxystore.endpoint.serve import serve
from proxystore.endpoint.storage import MemoryStorage
from proxystore.utils.environment import hostname
from testing.endpoint import terminate_process
from testing.endpoint import wait_for_endpoint
from testing.endpoint import write_endpoint


def _endpoint_dir(
    path: pathlib.Path,
    **kwargs: Any,
) -> tuple[EndpointDir, EndpointConfig]:
    options: dict[str, Any] = {'host': '127.0.0.1'}
    options.update(kwargs)
    return write_endpoint(str(path), 'my-endpoint', **options)


async def test_service(tmp_path: pathlib.Path) -> None:
    endpoint_dir, config = _endpoint_dir(tmp_path)
    connection_file = endpoint_dir.connection_path

    async with EndpointService(endpoint_dir) as service:
        assert service.dispatcher.id == config.id
        assert stat.S_IMODE(os.stat(connection_file).st_mode) == 0o600
        client = await asyncio.to_thread(EndpointClient.from_dir, endpoint_dir)
        assert client.info.id == config.id

    # Open connections are closed and the connection file is removed on
    # shutdown
    with pytest.raises(EndpointConnectionError):
        await asyncio.to_thread(client.exists, 'key')
    assert not os.path.exists(connection_file)


@pytest.mark.parametrize(
    ('backend', 'max_object_size', 'expected'),
    (('memory', 0, None), ('sqlite', 0, None), ('memory', 100, 100)),
)
async def test_service_object_size_limit(
    backend: Any,
    max_object_size: int,
    expected: int | None,
    tmp_path: pathlib.Path,
) -> None:
    endpoint_dir, _ = _endpoint_dir(
        tmp_path,
        storage=EndpointStorageConfig(backend=backend),
        max_object_size=max_object_size,
    )

    async with EndpointService(endpoint_dir):
        client = await asyncio.to_thread(EndpointClient.from_dir, endpoint_dir)
        assert client.info.max_object_size == expected
        # An object larger than the limit can be set if there is no limit
        data = b'x' * 1000
        if expected is None:
            await asyncio.to_thread(client.set, 'key', data)
            assert await asyncio.to_thread(client.get, 'key') == data
        client.close()


async def test_service_restricts_endpoint_dir(
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    endpoint_dir, _ = _endpoint_dir(tmp_path)
    os.chmod(endpoint_dir.path, 0o777)
    async with EndpointService(endpoint_dir):
        pass
    assert stat.S_IMODE(os.stat(endpoint_dir.path).st_mode) == 0o700
    assert any('other permissions' in r.message for r in caplog.records)


async def test_service_secret_key_mismatch(
    tmp_path: pathlib.Path,
) -> None:
    endpoint_dir, _ = _endpoint_dir(tmp_path)
    endpoint_dir.write_secret_key(SecretKey.generate())
    with pytest.raises(ValueError, match='does not match the secret key'):
        async with EndpointService(endpoint_dir):
            pass  # pragma: no cover
    assert not os.path.exists(endpoint_dir.connection_path)


async def test_service_port_in_use(tmp_path: pathlib.Path) -> None:
    endpoint_dir, _ = _endpoint_dir(tmp_path)
    async with EndpointService(endpoint_dir):
        running = endpoint_dir.read_connection()
        # A second instance fails to start without replacing or removing
        # the connection file of the running instance
        with pytest.raises(OSError, match='address already in use'):
            async with EndpointService(endpoint_dir):
                pass  # pragma: no cover
        assert endpoint_dir.read_connection() == running
        client = await asyncio.to_thread(EndpointClient.from_dir, endpoint_dir)
        await asyncio.to_thread(client.close)


async def test_service_start_up_failure_cleans_up(
    tmp_path: pathlib.Path,
) -> None:
    endpoint_dir, _ = _endpoint_dir(tmp_path)
    # The connection file cannot be written if its path is a directory
    os.mkdir(endpoint_dir.connection_path)
    with (
        mock.patch.object(MemoryStorage, 'close', AsyncMock()) as mock_close,
        pytest.raises(IsADirectoryError),
    ):
        async with EndpointService(endpoint_dir):
            pass  # pragma: no cover
    mock_close.assert_awaited_once()


@pytest.mark.parametrize(
    ('host', 'expected'),
    (('127.0.0.1', '127.0.0.1'), ('ip', '127.0.0.1'), ('fqdn', 'localhost')),
)
async def test_service_resolves_host(
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
        async with EndpointService(endpoint_dir):
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


async def test_service_tls(tmp_path: pathlib.Path) -> None:
    endpoint_dir, config = _endpoint_dir(tmp_path, tls=True)

    async with EndpointService(endpoint_dir):
        assert endpoint_dir.read_connection().tls_fingerprint is not None
        # The TLS certificate and key are not written to the directory
        assert not any('tls' in f for f in os.listdir(endpoint_dir.path))
        client = await asyncio.to_thread(EndpointClient.from_dir, endpoint_dir)
        assert isinstance(client._socket, ssl.SSLSocket)
        assert client.info.id == config.id
        await asyncio.to_thread(client.close)


async def test_service_peering(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.INFO)
    endpoint_dir, _ = _endpoint_dir(
        tmp_path,
        p2p=EndpointP2PConfig(enabled=True),
    )
    async with EndpointService(endpoint_dir) as service:
        peer_manager = service.dispatcher.peer_manager
        assert peer_manager is not None
        assert peer_manager.id == service.dispatcher.id
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


async def test_service_peering_addr_cache(
    tmp_path: pathlib.Path,
) -> None:
    endpoint_dir, _ = _endpoint_dir(
        tmp_path,
        p2p=EndpointP2PConfig(enabled=True, relays='none'),
    )
    async with EndpointService(endpoint_dir) as service:
        peer_manager = service.dispatcher.peer_manager
        assert peer_manager is not None
        assert peer_manager._addr_cache is not None
        path = peer_manager._addr_cache.path
        assert path == endpoint_dir.peer_addrs_path


@pytest.mark.parametrize('relative', (True, False, None))
async def test_service_database_path(
    relative: bool | None,
    tmp_path: pathlib.Path,
) -> None:
    other = tmp_path / 'lustre'
    other.mkdir()
    path = {True: 'blobs.db', False: str(other / 'blobs.db'), None: None}
    storage = EndpointStorageConfig(
        backend='sqlite',
        database_path=path[relative],
    )
    endpoint_dir, _ = _endpoint_dir(tmp_path, storage=storage)
    async with EndpointService(endpoint_dir) as service:
        await service.dispatcher.storage.set('key', b'value')
    expected = (
        str(other / 'blobs.db')
        if relative is False
        else os.path.join(endpoint_dir.path, 'blobs.db')
    )
    assert os.path.isfile(expected)


async def test_service_lifecycle(tmp_path: pathlib.Path) -> None:
    endpoint_dir, config = _endpoint_dir(tmp_path)
    service = EndpointService(endpoint_dir)
    assert not service.running
    for attr in ('config', 'dispatcher', 'connection'):
        with pytest.raises(RuntimeError, match='not running'):
            getattr(service, attr)
    # Stopping a service that is not running is a no-op
    await service.stop()

    await service.start()
    try:
        assert service.running
        assert service.config == config
        assert service.dispatcher.id == config.id
        assert service.connection == endpoint_dir.read_connection()
        with pytest.raises(RuntimeError, match='already running'):
            await service.start()
    finally:
        await service.stop()
    assert not service.running
    assert not os.path.exists(endpoint_dir.connection_path)

    # The service can be started again after it is stopped
    async with service:
        assert service.running
