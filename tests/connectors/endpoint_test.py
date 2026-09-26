from __future__ import annotations

import asyncio
import logging
import pathlib
import threading
import uuid
from typing import Any
from unittest import mock

import pytest

from proxystore.connectors.endpoint import _ConnectionPool
from proxystore.connectors.endpoint import _reset_pools_after_fork
from proxystore.connectors.endpoint import EndpointConnector
from proxystore.connectors.endpoint import EndpointKey
from proxystore.endpoint.auth import generate_token
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.exceptions import EndpointConnectionError
from proxystore.endpoint.exceptions import EndpointConnectorError
from proxystore.endpoint.exceptions import EndpointNotRunningError
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.serve import running_endpoint
from testing.compat import randbytes
from testing.endpoint import copy_endpoint_dir
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


def test_endpoint_not_started(tmp_path: pathlib.Path) -> None:
    endpoint_uuid = uuid.uuid4()
    config = EndpointConfig(name='test', uuid=str(endpoint_uuid), port=1)
    EndpointDir(str(tmp_path / 'test')).write_config(config)

    with pytest.raises(EndpointConnectorError) as exc_info:
        EndpointConnector(
            endpoints=[endpoint_uuid],
            proxystore_dir=str(tmp_path),
        )
    message = str(exc_info.value)
    assert 'Failed to connect' in message
    assert f'test ({endpoint_uuid})' in message
    assert 'Is the endpoint running?' in message


def test_endpoint_wrong_token(
    endpoint: EndpointConfig,
    endpoint_dir: EndpointDir,
    tmp_path: pathlib.Path,
) -> None:
    copied_dir = copy_endpoint_dir(endpoint_dir, str(tmp_path))
    info = copied_dir.read_connection()
    copied_dir.write_connection(info._replace(token=generate_token()))

    with pytest.raises(EndpointConnectorError, match='failed to prove'):
        EndpointConnector([endpoint.uuid], proxystore_dir=str(tmp_path))


def test_endpoint_uuid_mismatch(
    endpoint: EndpointConfig,
    endpoint_dir: EndpointDir,
    tmp_path: pathlib.Path,
) -> None:
    # Config has a different UUID than the endpoint running on the host/port
    copied_dir = copy_endpoint_dir(endpoint_dir, str(tmp_path))
    config = copied_dir.read_config()
    config.uuid = str(uuid.uuid4())
    copied_dir.write_config(config)

    with pytest.raises(EndpointConnectorError, match='Expected endpoint'):
        EndpointConnector([config.uuid], proxystore_dir=str(tmp_path))


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


def test_connection_pool_retries_request_once() -> None:
    connections: list[_FakeClient] = []

    def _connect() -> _FakeClient:
        connections.append(_FakeClient(fail=True))
        return connections[-1]

    pool = _ConnectionPool(_connect)  # type: ignore[arg-type]
    with pytest.raises(EndpointConnectionError):
        pool.run(_fake_request)
    assert len(connections) == 2
    assert len(pool._idle) == 0


def test_connection_pool_reconnect_backoff() -> None:
    attempts = 0

    def _connect() -> _FakeClient:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise EndpointNotRunningError('not running')
        return _FakeClient(fail=False)

    pool = _ConnectionPool(_connect, reconnect_timeout=5)  # type: ignore[arg-type]
    assert pool.run(_fake_request) == 'ok'
    assert attempts == 3


def test_connection_pool_reconnect_timeout() -> None:
    attempts = 0

    def _connect() -> _FakeClient:
        nonlocal attempts
        attempts += 1
        raise EndpointNotRunningError('not running')

    pool = _ConnectionPool(_connect, reconnect_timeout=0.05)  # type: ignore[arg-type]
    with pytest.raises(EndpointNotRunningError):
        pool.run(_fake_request)
    assert attempts > 1

    # Other errors are not retried
    connect = mock.MagicMock(side_effect=EndpointProtocolError('bad'))
    pool = _ConnectionPool(connect, reconnect_timeout=5)
    with pytest.raises(EndpointProtocolError):
        pool.run(_fake_request)
    assert connect.call_count == 1


def test_connection_pool_discards_interrupted_connection() -> None:
    pool = _ConnectionPool(lambda: _FakeClient(fail=False))  # type: ignore[arg-type,return-value]
    client = _FakeClient(fail=False)
    pool.add(client)  # type: ignore[arg-type]

    def _interrupted(acquired: Any) -> None:
        acquired.close()
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        pool.run(_interrupted)
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
    endpoint_dir = EndpointDir(str(tmp_path / config.name))
    endpoint_dir.write_config(config)

    async with running_endpoint(endpoint_dir):
        connector = await asyncio.to_thread(
            EndpointConnector,
            [config.uuid],
            proxystore_dir=str(tmp_path),
        )
        key = await asyncio.to_thread(connector.put, b'value')

    # The idle connection in the pool was closed by the endpoint and the
    # endpoint has a new token after restarting.
    async with running_endpoint(endpoint_dir):
        assert not await asyncio.to_thread(connector.exists, key)
        assert any('Retrying' in r.message for r in caplog.records)

    # A request made while the endpoint is stopped succeeds once the
    # endpoint restarts within the reconnect timeout
    request = asyncio.create_task(asyncio.to_thread(connector.exists, key))
    await asyncio.sleep(0.2)
    assert not request.done()
    async with running_endpoint(endpoint_dir):
        assert not await request
    connector.close()


async def test_connector_tls(tmp_path: pathlib.Path) -> None:
    config = EndpointConfig(
        name='tls-endpoint',
        uuid=str(uuid.uuid4()),
        host='127.0.0.1',
        port=open_port(),
        tls=True,
    )
    endpoint_dir = EndpointDir(str(tmp_path / config.name))
    endpoint_dir.write_config(config)

    def _run() -> None:
        with EndpointConnector(
            [config.uuid],
            proxystore_dir=str(tmp_path),
        ) as connector:
            key = connector.put(b'value')
            assert connector.get(key) == b'value'

    async with running_endpoint(endpoint_dir):
        await asyncio.to_thread(_run)
