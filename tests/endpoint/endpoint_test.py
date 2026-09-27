from __future__ import annotations

import asyncio
import logging
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
from proxystore.endpoint.exceptions import EndpointConnectionError
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.identity import SecretKey
from proxystore.endpoint.p2p.manager import PeerOptions
from proxystore.endpoint.storage import MemoryStorage
from proxystore.endpoint.storage import SQLiteStorage
from proxystore.utils.environment import hostname
from testing.endpoint import write_endpoint
from testing.p2p import LOCAL_PEER_OPTIONS


def _endpoint_dir(
    path: pathlib.Path,
    **kwargs: Any,
) -> tuple[EndpointDir, EndpointConfig]:
    options: dict[str, Any] = {'host': '127.0.0.1'}
    options.update(kwargs)
    return write_endpoint(str(path), 'my-endpoint', **options)


async def test_endpoint(tmp_path: pathlib.Path) -> None:
    endpoint_dir, config = _endpoint_dir(tmp_path)
    connection_file = endpoint_dir.connection_path

    async with Endpoint(endpoint_dir) as endpoint:
        assert endpoint.dispatcher.id == config.id
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
async def test_endpoint_object_size_limit(
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

    async with Endpoint(endpoint_dir):
        client = await asyncio.to_thread(EndpointClient.from_dir, endpoint_dir)
        assert client.info.max_object_size == expected
        # An object larger than the limit can be set if there is no limit
        data = b'x' * 1000
        if expected is None:
            await asyncio.to_thread(client.set, 'key', data)
            assert await asyncio.to_thread(client.get, 'key') == data
        client.close()


async def test_endpoint_restricts_endpoint_dir(
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    endpoint_dir, _ = _endpoint_dir(tmp_path)
    os.chmod(endpoint_dir.path, 0o777)
    async with Endpoint(endpoint_dir):
        pass
    assert stat.S_IMODE(os.stat(endpoint_dir.path).st_mode) == 0o700
    assert any('other permissions' in r.message for r in caplog.records)


async def test_endpoint_secret_key_mismatch(
    tmp_path: pathlib.Path,
) -> None:
    endpoint_dir, _ = _endpoint_dir(tmp_path)
    endpoint_dir.write_secret_key(SecretKey.generate())
    with pytest.raises(ValueError, match='does not match the secret key'):
        async with Endpoint(endpoint_dir):
            pass  # pragma: no cover
    assert not os.path.exists(endpoint_dir.connection_path)


async def test_endpoint_port_in_use(tmp_path: pathlib.Path) -> None:
    endpoint_dir, _ = _endpoint_dir(tmp_path)
    async with Endpoint(endpoint_dir):
        running = endpoint_dir.read_connection()
        # A second instance fails to start without replacing or removing
        # the connection file of the running instance
        with pytest.raises(OSError, match='address already in use'):
            async with Endpoint(endpoint_dir):
                pass  # pragma: no cover
        assert endpoint_dir.read_connection() == running
        client = await asyncio.to_thread(EndpointClient.from_dir, endpoint_dir)
        await asyncio.to_thread(client.close)


async def test_endpoint_start_up_failure_cleans_up(
    tmp_path: pathlib.Path,
) -> None:
    endpoint_dir, _ = _endpoint_dir(tmp_path)
    # The connection file cannot be written if its path is a directory
    os.mkdir(endpoint_dir.connection_path)
    with (
        mock.patch.object(MemoryStorage, 'close', AsyncMock()) as mock_close,
        pytest.raises(IsADirectoryError),
    ):
        async with Endpoint(endpoint_dir):
            pass  # pragma: no cover
    mock_close.assert_awaited_once()


@pytest.mark.parametrize(
    ('host', 'expected'),
    (('127.0.0.1', '127.0.0.1'), ('ip', '127.0.0.1'), ('fqdn', 'localhost')),
)
async def test_endpoint_resolves_host(
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
        async with Endpoint(endpoint_dir):
            info = endpoint_dir.read_connection()
            assert info.host == expected
            assert info.hostname == hostname()
            assert info.pid == os.getpid()
    # The configuration is never modified by the endpoint
    with open(endpoint_dir.config_path, 'rb') as f:
        assert f.read() == before


async def test_endpoint_tls(tmp_path: pathlib.Path) -> None:
    endpoint_dir, config = _endpoint_dir(tmp_path, tls=True)

    async with Endpoint(endpoint_dir):
        assert endpoint_dir.read_connection().tls_fingerprint is not None
        # The TLS certificate and key are not written to the directory
        assert not any('tls' in f for f in os.listdir(endpoint_dir.path))
        client = await asyncio.to_thread(EndpointClient.from_dir, endpoint_dir)
        assert isinstance(client._socket, ssl.SSLSocket)
        assert client.info.id == config.id
        await asyncio.to_thread(client.close)


async def test_endpoint_peering(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.INFO)
    endpoint_dir, _ = _endpoint_dir(
        tmp_path,
        p2p=EndpointP2PConfig(enabled=True),
    )
    async with Endpoint(endpoint_dir) as endpoint:
        peer_manager = endpoint.peer_manager
        assert peer_manager is not None
        assert peer_manager.id == endpoint.id
    assert any('Loaded 0 peer(s)' in r.message for r in caplog.records)


@pytest.mark.parametrize(
    'relays',
    ('n0', 'none', ['https://relay.example.com']),
)
def test_peer_options_from_config(relays: Any) -> None:
    options = PeerOptions.from_config(EndpointP2PConfig(relays=relays))
    assert isinstance(options.preset, iroh.Preset)
    if relays == 'n0':
        assert options.relay_mode is None
        assert options.online_timeout is not None
    else:
        assert isinstance(options.relay_mode, iroh.RelayMode)
    if relays == 'none':
        assert options.online_timeout is None


async def test_endpoint_peering_addr_cache(
    tmp_path: pathlib.Path,
) -> None:
    endpoint_dir, _ = _endpoint_dir(
        tmp_path,
        p2p=EndpointP2PConfig(enabled=True, relays='none'),
    )
    async with Endpoint(endpoint_dir) as endpoint:
        peer_manager = endpoint.peer_manager
        assert peer_manager is not None
        assert peer_manager._addr_cache is not None
        path = peer_manager._addr_cache.path
        assert path == endpoint_dir.peer_addrs_path


@pytest.mark.parametrize('relative', (True, False, None))
async def test_endpoint_database_path(
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
    async with Endpoint(endpoint_dir) as endpoint:
        await endpoint.dispatcher.storage.set('key', b'value')
    expected = (
        str(other / 'blobs.db')
        if relative is False
        else os.path.join(endpoint_dir.path, 'blobs.db')
    )
    assert os.path.isfile(expected)


async def test_endpoint_lifecycle(tmp_path: pathlib.Path) -> None:
    endpoint_dir, config = _endpoint_dir(tmp_path)
    endpoint = Endpoint(endpoint_dir)
    assert not endpoint.running
    assert repr(endpoint) == f'Endpoint({endpoint_dir.path!r})'
    for attr in ('config', 'id', 'name', 'dispatcher', 'connection'):
        with pytest.raises(RuntimeError, match='not running'):
            getattr(endpoint, attr)
    # Stopping an endpoint that is not running is a no-op
    await endpoint.stop()

    await endpoint.start()
    try:
        assert endpoint.running
        assert endpoint.config == config
        assert endpoint.id == config.id
        assert endpoint.name == config.name
        assert endpoint.dispatcher.id == config.id
        assert endpoint.peer_manager is None
        assert endpoint.connection == endpoint_dir.read_connection()
        with pytest.raises(RuntimeError, match='already running'):
            await endpoint.start()
    finally:
        await endpoint.stop()
    assert not endpoint.running
    assert not os.path.exists(endpoint_dir.connection_path)

    # The endpoint can be started again after it is stopped
    async with endpoint:
        assert endpoint.running


async def test_endpoint_storage_override(tmp_path: pathlib.Path) -> None:
    storage = SQLiteStorage(':memory:')
    endpoint_dir, _ = _endpoint_dir(tmp_path)
    with mock.patch.object(storage, 'close', AsyncMock()) as mock_close:
        async with Endpoint(endpoint_dir, storage=storage) as endpoint:
            assert endpoint.dispatcher.storage is storage
    # The endpoint closes the storage it was given
    mock_close.assert_awaited_once()


class _AllowAll:
    def allowed(self, peer_id: EndpointId) -> bool:
        return True

    def name_of(self, peer_id: EndpointId) -> str | None:
        return 'peer'

    def revoked(self) -> set[EndpointId]:
        return set()


async def test_endpoint_peer_overrides(tmp_path: pathlib.Path) -> None:
    endpoint_dir, _ = _endpoint_dir(
        tmp_path,
        p2p=EndpointP2PConfig(enabled=True, relays='none'),
    )
    policy = _AllowAll()
    async with Endpoint(
        endpoint_dir,
        peer_policy=policy,
        peer_options=LOCAL_PEER_OPTIONS,
    ) as endpoint:
        assert endpoint.peer_manager is not None
        assert endpoint.peer_manager.policy is policy
        assert endpoint.peer_manager.options is LOCAL_PEER_OPTIONS
        assert endpoint.peer_manager.id == endpoint.id


async def test_endpoint_reads_config_once(tmp_path: pathlib.Path) -> None:
    endpoint_dir, _ = _endpoint_dir(
        tmp_path,
        p2p=EndpointP2PConfig(enabled=True, relays='none'),
    )
    with mock.patch.object(
        EndpointDir,
        'read_config',
        autospec=True,
        side_effect=EndpointDir.read_config,
    ) as read_config:
        async with Endpoint(endpoint_dir, peer_options=LOCAL_PEER_OPTIONS):
            pass
    read_config.assert_called_once()
