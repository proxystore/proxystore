from __future__ import annotations

import asyncio
import logging
import os
import pathlib
import threading
import uuid
from typing import Any

import pytest

from proxystore.connectors.endpoint import _ConnectionPool
from proxystore.connectors.endpoint import _reset_pools_after_fork
from proxystore.connectors.endpoint import EndpointConnector
from proxystore.connectors.endpoint import EndpointConnectorError
from proxystore.connectors.endpoint import EndpointKey
from proxystore.endpoint.auth import generate_token_file
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import EndpointFiles
from proxystore.endpoint.config import read_config
from proxystore.endpoint.config import write_config
from proxystore.endpoint.exceptions import EndpointConnectionError
from proxystore.endpoint.serve import _serve_async
from testing.compat import randbytes
from testing.endpoint import copy_endpoint_dir
from testing.endpoint import wait_for_endpoint
from testing.utils import open_port


def test_no_endpoints_provided() -> None:
    with pytest.raises(ValueError):
        EndpointConnector(endpoints=[])


def test_no_endpoints_match(endpoint_connector) -> None:
    with pytest.raises(EndpointConnectorError, match='Failed to find'):
        EndpointConnector(
            endpoints=[str(uuid.uuid4())],
            proxystore_dir=endpoint_connector.config()['proxystore_dir'],
        )


def test_endpoint_not_started(tmp_path: pathlib.Path, caplog) -> None:
    endpoint_uuid = uuid.uuid4()
    config = EndpointConfig(
        name='test',
        uuid=str(endpoint_uuid),
        port=1,
        host=None,
    )
    config_path = tmp_path / 'test'
    write_config(config, str(config_path))

    caplog.set_level(logging.INFO)
    with pytest.raises(EndpointConnectorError, match='Failed to find'):
        EndpointConnector(
            endpoints=[endpoint_uuid],
            proxystore_dir=str(tmp_path),
        )

    message = (
        f'Found valid configuration for endpoint "test" ({endpoint_uuid}), '
        'but the endpoint has not been started'
    )
    assert any([message == record.message for record in caplog.records])


def test_endpoint_missing_token(
    endpoint: EndpointConfig,
    endpoint_dir: str,
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    copied_dir = copy_endpoint_dir(endpoint_dir, str(tmp_path))
    os.remove(EndpointFiles(copied_dir).token)

    caplog.set_level(logging.DEBUG)
    with pytest.raises(EndpointConnectorError, match='Failed to find'):
        EndpointConnector([endpoint.uuid], proxystore_dir=str(tmp_path))
    assert any('No such file' in r.message for r in caplog.records)


def test_endpoint_wrong_token(
    endpoint: EndpointConfig,
    endpoint_dir: str,
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    copied_dir = copy_endpoint_dir(endpoint_dir, str(tmp_path))
    generate_token_file(EndpointFiles(copied_dir).token)

    caplog.set_level(logging.WARNING)
    with pytest.raises(EndpointConnectorError, match='Failed to find'):
        EndpointConnector([endpoint.uuid], proxystore_dir=str(tmp_path))
    assert any('failed to prove' in r.message for r in caplog.records)


def test_endpoint_uuid_mismatch(
    endpoint: EndpointConfig,
    endpoint_dir: str,
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    # Config has a different UUID than the endpoint running on the host/port
    copied_dir = copy_endpoint_dir(endpoint_dir, str(tmp_path))
    config = read_config(copied_dir)
    config.uuid = str(uuid.uuid4())
    write_config(config, copied_dir)

    caplog.set_level(logging.DEBUG)
    with pytest.raises(EndpointConnectorError, match='Failed to find'):
        EndpointConnector([config.uuid], proxystore_dir=str(tmp_path))
    assert any('different UUID' in r.message for r in caplog.records)


def test_request_error(endpoint_connector) -> None:
    connector = EndpointConnector.from_config(endpoint_connector.config())
    key = EndpointKey(object_id='key', endpoint_id='not-a-uuid')

    with pytest.raises(EndpointConnectorError, match='Evict failed'):
        connector.evict(key)
    with pytest.raises(EndpointConnectorError, match='Exists failed'):
        connector.exists(key)
    with pytest.raises(EndpointConnectorError, match='Get failed'):
        connector.get(key)
    with pytest.raises(EndpointConnectorError, match='Set failed'):
        connector.set(key, b'value')

    connector.close()


def test_large_data(endpoint_connector) -> None:
    connector = EndpointConnector.from_config(endpoint_connector.config())
    data = randbytes(5_000_000)
    key = connector.put(data)
    assert connector.get(key) == data
    connector.close()


def test_connection_reuse(endpoint_connector) -> None:
    connector = EndpointConnector.from_config(endpoint_connector.config())
    assert len(connector._pool._idle) == 1
    client = connector._pool._idle[0]

    key = connector.put(b'value')
    assert connector.exists(key)
    assert len(connector._pool._idle) == 1
    assert connector._pool._idle[0] is client

    connector.close()
    assert len(connector._pool._idle) == 0
    assert client.closed


def test_connection_pool_concurrent_requests(endpoint_connector) -> None:
    connector = EndpointConnector.from_config(endpoint_connector.config())
    barrier = threading.Barrier(4)
    errors: list[Exception] = []

    def _worker() -> None:
        try:
            barrier.wait()
            for _ in range(20):
                key = connector.put(b'value')
                assert connector.get(key) == b'value'
        except Exception as e:  # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=_worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    assert 1 <= len(connector._pool._idle) <= 4
    connector.close()


def test_connection_pool_after_fork(endpoint_connector) -> None:
    connector = EndpointConnector.from_config(endpoint_connector.config())
    parent_client = connector._pool._idle[0]
    parent_lock = connector._pool._lock

    # Simulate the fork handler running in a forked child process
    _reset_pools_after_fork()
    assert parent_client.closed
    assert len(connector._pool._idle) == 0
    assert connector._pool._lock is not parent_lock

    assert connector.exists(connector.new_key()) is False
    assert len(connector._pool._idle) == 1
    connector.close()


def test_closed_connections_not_returned_to_pool(endpoint_connector) -> None:
    connector = EndpointConnector.from_config(endpoint_connector.config())
    client = connector._pool._idle[0]
    client.close()

    assert connector._pool.run(lambda acquired: acquired) is client
    assert len(connector._pool._idle) == 0

    # A new connection is created when the pool is empty
    assert not connector.exists(connector.new_key())
    assert len(connector._pool._idle) == 1
    connector.close()


class _FakeClient:
    def __init__(self, fail: bool) -> None:
        self.closed = False
        self.fail = fail

    def close(self) -> None:
        self.closed = True


def _fake_request(client: Any) -> str:
    if client.fail:
        client.close()
        raise EndpointConnectionError('connection lost')
    return 'ok'


def test_connection_pool_retries_closed_idle_connection() -> None:
    pool = _ConnectionPool(lambda: _FakeClient(fail=False))  # type: ignore[arg-type,return-value]
    stale = _FakeClient(fail=True)
    pool.add(stale)  # type: ignore[arg-type]

    assert pool.run(_fake_request) == 'ok'
    assert stale.closed
    assert len(pool._idle) == 1
    assert pool._idle[0] is not stale


def test_connection_pool_does_not_retry_new_connection() -> None:
    connections: list[_FakeClient] = []

    def _connect() -> _FakeClient:
        connections.append(_FakeClient(fail=True))
        return connections[-1]

    pool = _ConnectionPool(_connect)  # type: ignore[arg-type]
    with pytest.raises(EndpointConnectionError):
        pool.run(_fake_request)
    assert len(connections) == 1

    # Only retried once if a new connection also fails
    pool.add(_FakeClient(fail=True))  # type: ignore[arg-type]
    with pytest.raises(EndpointConnectionError):
        pool.run(_fake_request)
    assert len(connections) == 2
    assert len(pool._idle) == 0


async def test_connector_endpoint_restart(
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG, logger='proxystore.connectors.endpoint')
    config = EndpointConfig(
        name='restart-endpoint',
        uuid=str(uuid.uuid4()),
        host='127.0.0.1',
        port=open_port(),
    )
    endpoint_dir = str(tmp_path / config.name)
    write_config(config, endpoint_dir)

    async def _start() -> tuple[asyncio.Event, asyncio.Task[None]]:
        stop = asyncio.Event()
        task = asyncio.create_task(_serve_async(config, endpoint_dir, stop))
        await asyncio.to_thread(wait_for_endpoint, '127.0.0.1', config.port)
        return stop, task

    stop, task = await _start()
    connector = await asyncio.to_thread(
        EndpointConnector,
        [config.uuid],
        proxystore_dir=str(tmp_path),
    )
    try:
        key = await asyncio.to_thread(connector.put, b'value')
        stop.set()
        await task

        # The idle connection in the pool was closed by the endpoint and
        # the endpoint has a new token after restarting.
        stop, task = await _start()
        assert not await asyncio.to_thread(connector.exists, key)
        assert any('Retrying' in r.message for r in caplog.records)
    finally:
        connector.close()
        stop.set()
        await task


async def test_connector_tls(tmp_path: pathlib.Path) -> None:
    config = EndpointConfig(
        name='tls-endpoint',
        uuid=str(uuid.uuid4()),
        host='127.0.0.1',
        port=open_port(),
        tls=True,
    )
    endpoint_dir = str(tmp_path / config.name)
    write_config(config, endpoint_dir)

    stop = asyncio.Event()
    task = asyncio.create_task(_serve_async(config, endpoint_dir, stop))
    await asyncio.to_thread(wait_for_endpoint, '127.0.0.1', config.port)

    def _run() -> None:
        with EndpointConnector(
            [config.uuid],
            proxystore_dir=str(tmp_path),
        ) as connector:
            key = connector.put(b'value')
            assert connector.get(key) == b'value'

    try:
        await asyncio.to_thread(_run)
    finally:
        stop.set()
        await task
