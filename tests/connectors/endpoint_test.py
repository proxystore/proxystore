from __future__ import annotations

import asyncio
import logging
import os
import pathlib
import threading
from random import randbytes
from typing import Any
from unittest import mock

import pytest

from proxystore.connectors.endpoint import _ConnectionPool
from proxystore.connectors.endpoint import _reset_pools_after_fork
from proxystore.connectors.endpoint import EndpointConnector
from proxystore.connectors.endpoint import EndpointConnectorError
from proxystore.connectors.endpoint import EndpointKey
from proxystore.endpoint.auth import EndpointToken
from proxystore.endpoint.client import REQUEST_TIMEOUT
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.directory import EndpointStatus
from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.exceptions import EndpointConnectionError
from proxystore.endpoint.exceptions import EndpointError
from proxystore.endpoint.exceptions import EndpointNotRunningError
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.exceptions import EndpointTimeoutError
from proxystore.endpoint.identity import EndpointId
from proxystore.serialize import BytesLike
from testing.endpoint import copy_endpoint_dir
from testing.endpoint import write_endpoint
from testing.utils import wait_until


def test_no_endpoints_provided() -> None:
    with pytest.raises(ValueError, match='At least one endpoint'):
        EndpointConnector(endpoints=[])


def test_no_endpoints_match(endpoint_connector) -> None:
    with pytest.raises(EndpointConnectorError, match='Failed to find'):
        EndpointConnector(
            endpoints=[EndpointId.random()],
            proxystore_dir=endpoint_connector.config()['proxystore_dir'],
        )


def test_endpoints_default_single_running(
    endpoint: EndpointConfig,
    endpoint_dir: EndpointDir,
) -> None:
    home = os.path.dirname(endpoint_dir.path)
    with EndpointConnector(proxystore_dir=home) as connector:
        assert connector.endpoint_id == endpoint.id
        assert connector.config()['endpoints'] is None
        assert connector.config()['request_timeout'] == REQUEST_TIMEOUT
        key = connector.put(b'value')
        assert connector.get(key) == b'value'

        with EndpointConnector.from_config(connector.config()) as other:
            assert other.endpoint_id == endpoint.id


def test_endpoints_default_none_running(tmp_path: pathlib.Path) -> None:
    write_endpoint(str(tmp_path), 'stopped')

    with pytest.raises(EndpointConnectorError, match='running endpoint'):
        EndpointConnector(proxystore_dir=tmp_path)


def test_endpoints_default_multiple_running(tmp_path: pathlib.Path) -> None:
    configs = [write_endpoint(str(tmp_path), f'ep{i}')[1] for i in range(2)]

    with (
        mock.patch.object(
            EndpointDir,
            'status',
            return_value=EndpointStatus.RUNNING,
        ),
        pytest.raises(EndpointConnectorError) as exc_info,
    ):
        EndpointConnector(proxystore_dir=tmp_path)
    message = str(exc_info.value)
    assert 'multiple running endpoints' in message
    for config in configs:
        assert f'{config.name} ({config.id})' in message


def test_endpoints_default_other_host(
    endpoint: EndpointConfig,
    endpoint_dir: EndpointDir,
    tmp_path: pathlib.Path,
) -> None:
    # Simulate an endpoint started on another host which shares the
    # ProxyStore home directory, such as the login node of a cluster.
    copied_dir = copy_endpoint_dir(endpoint_dir, str(tmp_path))
    info = copied_dir.read_connection()
    copied_dir.write_connection(info.model_copy(update={'hostname': 'other'}))
    assert copied_dir.status() == EndpointStatus.OTHER_HOST

    with EndpointConnector(proxystore_dir=tmp_path) as connector:
        assert connector.endpoint_id == endpoint.id
        key = connector.put(b'value')
        assert connector.get(key) == b'value'
        connector.evict(key)


def _mock_statuses(
    statuses: dict[str, EndpointStatus],
) -> mock._patch[mock.MagicMock]:
    return mock.patch.object(
        EndpointDir,
        'status',
        autospec=True,
        side_effect=lambda endpoint_dir: statuses[endpoint_dir.name],
    )


def test_endpoints_default_running_preferred(tmp_path: pathlib.Path) -> None:
    _, local = write_endpoint(str(tmp_path), 'local')
    write_endpoint(str(tmp_path), 'remote')
    statuses = {
        'local': EndpointStatus.RUNNING,
        'remote': EndpointStatus.OTHER_HOST,
    }

    with (
        _mock_statuses(statuses),
        mock.patch(
            'proxystore.connectors.endpoint._connect',
            return_value=mock.MagicMock(),
        ) as mock_connect,
    ):
        connector = EndpointConnector(proxystore_dir=tmp_path)
    assert connector.endpoint_id == local.id
    # The endpoint started on another host is never tried.
    mock_connect.assert_called_once()


def test_endpoints_default_running_unreachable(
    tmp_path: pathlib.Path,
) -> None:
    write_endpoint(str(tmp_path), 'local')

    with (
        _mock_statuses({'local': EndpointStatus.RUNNING}),
        pytest.raises(EndpointConnectorError) as exc_info,
    ):
        EndpointConnector(proxystore_dir=tmp_path)
    message = str(exc_info.value)
    assert 'Failed to connect to the endpoint running' in message
    assert 'local (' in message


def test_endpoints_default_other_host_unreachable(
    tmp_path: pathlib.Path,
) -> None:
    _, config = write_endpoint(str(tmp_path), 'remote')
    write_endpoint(str(tmp_path), 'stopped')
    statuses = {
        'remote': EndpointStatus.OTHER_HOST,
        'stopped': EndpointStatus.STOPPED,
    }

    with (
        _mock_statuses(statuses),
        pytest.raises(EndpointConnectorError) as exc_info,
    ):
        EndpointConnector(proxystore_dir=tmp_path)
    message = str(exc_info.value)
    assert 'Failed to find a running endpoint' in message
    assert 'could not be reached' in message
    assert f'remote ({config.id})' in message
    assert 'stopped' not in message


def test_endpoints_default_multiple_other_host(
    tmp_path: pathlib.Path,
) -> None:
    configs = [write_endpoint(str(tmp_path), f'ep{i}')[1] for i in range(3)]
    statuses = {
        'ep0': EndpointStatus.OTHER_HOST,
        'ep1': EndpointStatus.OTHER_HOST,
        'ep2': EndpointStatus.OTHER_HOST,
    }
    # The third endpoint cannot be reached.
    clients = {config.id: mock.MagicMock() for config in configs[:2]}

    def _connect(endpoint_dir, endpoint_id, request_timeout):
        if endpoint_id not in clients:
            raise EndpointConnectionError('unreachable')
        return clients[endpoint_id]

    with (
        _mock_statuses(statuses),
        mock.patch(
            'proxystore.connectors.endpoint._connect',
            side_effect=_connect,
        ),
        pytest.raises(EndpointConnectorError) as exc_info,
    ):
        EndpointConnector(proxystore_dir=tmp_path)
    message = str(exc_info.value)
    assert 'multiple endpoints started on other hosts' in message
    assert f'ep0 ({configs[0].id})' in message
    assert f'ep1 ({configs[1].id})' in message
    assert 'ep2' not in message
    for client in clients.values():
        client.close.assert_called_once()


def test_endpoint_not_started(tmp_path: pathlib.Path) -> None:
    endpoint_id = EndpointId.random()
    config = EndpointConfig(name='test', id=endpoint_id, port=1)
    EndpointDir(str(tmp_path / 'test')).write_config(config)

    with pytest.raises(EndpointConnectorError) as exc_info:
        EndpointConnector(
            endpoints=[endpoint_id],
            proxystore_dir=tmp_path,
        )
    message = str(exc_info.value)
    assert 'Failed to connect' in message
    assert f'test ({endpoint_id})' in message
    assert 'Is the endpoint running?' in message


def test_endpoint_wrong_token(
    endpoint: EndpointConfig,
    endpoint_dir: EndpointDir,
    tmp_path: pathlib.Path,
) -> None:
    copied_dir = copy_endpoint_dir(endpoint_dir, str(tmp_path))
    info = copied_dir.read_connection()
    copied_dir.write_connection(
        info.model_copy(update={'token': EndpointToken.generate()}),
    )

    with pytest.raises(EndpointConnectorError, match='failed to prove'):
        EndpointConnector([endpoint.id], proxystore_dir=tmp_path)


def test_endpoint_id_mismatch(
    endpoint: EndpointConfig,
    endpoint_dir: EndpointDir,
    tmp_path: pathlib.Path,
) -> None:
    # Config has a different ID than the endpoint running on the host/port
    copied_dir = copy_endpoint_dir(endpoint_dir, str(tmp_path))
    config = copied_dir.read_config()
    config.id = EndpointId.random()
    copied_dir.write_config(config)

    with pytest.raises(EndpointConnectorError, match='Expected endpoint'):
        EndpointConnector([config.id], proxystore_dir=tmp_path)


def test_request_error(endpoint_connector) -> None:
    connector = EndpointConnector.from_config(endpoint_connector.config())
    key = EndpointKey(object_id='key', endpoint_id='not-an-id')

    with pytest.raises(EndpointConnectorError, match='Evict failed') as e:
        connector.evict(key)
    # The connector owns its error type and keeps the internal cause
    assert not isinstance(e.value, EndpointError)
    assert e.value.__cause__ is not None
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
        except Exception as e:  # pragma: no cover  # noqa: BLE001
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


@pytest.mark.parametrize('clear', (None, False))
def test_close_without_clear_does_not_warn(
    clear: bool | None,
    endpoint_connector,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    connector = EndpointConnector.from_config(endpoint_connector.config())
    assert connector.close(clear=clear) is False
    assert 'does not remove objects' not in caplog.text


def test_close_never_clears(
    endpoint_connector,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    connector = EndpointConnector.from_config(endpoint_connector.config())
    key = connector.put(b'value')
    # Objects are owned by the endpoints so they are never removed.
    assert connector.close(clear=True) is False
    assert 'does not remove objects' in caplog.text
    assert endpoint_connector.get(key) == b'value'
    endpoint_connector.evict(key)


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


def test_request_timeout_must_be_positive(tmp_path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match='must be positive'):
        EndpointConnector(proxystore_dir=tmp_path, request_timeout=0)


def test_connection_pool_does_not_retry_timeout() -> None:
    connect = mock.MagicMock(side_effect=lambda: _FakeClient(fail=False))
    pool = _ConnectionPool(connect)

    def _timeout(client: Any) -> None:
        client.close()
        raise EndpointTimeoutError('timed out')

    with pytest.raises(EndpointTimeoutError):
        pool.run(_timeout)
    assert connect.call_count == 1
    assert len(pool._idle) == 0


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
    endpoint_dir, config = write_endpoint(
        str(tmp_path),
        'restart-endpoint',
        host='127.0.0.1',
    )

    async with Endpoint(endpoint_dir):
        connector = await asyncio.to_thread(
            EndpointConnector,
            [config.id],
            proxystore_dir=tmp_path,
        )
        # Paths are converted to str so the config is serializable.
        assert connector.config()['proxystore_dir'] == str(tmp_path)
        key = await asyncio.to_thread(connector.put, b'value')

    # The idle connection in the pool was closed by the endpoint and the
    # endpoint has a new token after restarting.
    async with Endpoint(endpoint_dir):
        assert not await asyncio.to_thread(connector.exists, key)
        assert any('Retrying' in r.message for r in caplog.records)

    # A request made while the endpoint is stopped succeeds once the
    # endpoint restarts within the reconnect timeout
    caplog.clear()
    request = asyncio.create_task(asyncio.to_thread(connector.exists, key))
    await wait_until(
        lambda: any(
            'Retrying connection' in r.message for r in caplog.records
        ),
    )
    assert not request.done()
    async with Endpoint(endpoint_dir):
        assert not await request
    connector.close()


async def test_connector_tls(tmp_path: pathlib.Path) -> None:
    endpoint_dir, config = write_endpoint(
        str(tmp_path),
        'tls-endpoint',
        host='127.0.0.1',
        tls=True,
    )

    def _run() -> None:
        with EndpointConnector(
            [config.id],
            proxystore_dir=tmp_path,
        ) as connector:
            key = connector.put(b'value')
            assert connector.get(key) == b'value'

    async with Endpoint(endpoint_dir):
        await asyncio.to_thread(_run)


def test_get_batch_local_keys_one_at_a_time(endpoint_connector) -> None:
    with EndpointConnector.from_config(endpoint_connector.config()) as conn:
        keys = conn.put_batch([b'a', b'b'])
        missing = conn.new_key()
        assert conn.get_batch([*keys, missing]) == [b'a', b'b', None]
        # Batches of local keys never start the threads for peer requests.
        assert conn._pool._executor is None


def test_get_batch_peer_keys_at_same_time(endpoint_connector) -> None:
    with EndpointConnector.from_config(endpoint_connector.config()) as conn:
        local = conn.put(b'local')
        peer = EndpointId.random()
        peer_keys = [EndpointKey(f'peer-{i}', peer) for i in range(2)]
        get = conn.get
        # Each peer request waits for the other, so this only finishes if
        # they run at the same time.
        barrier = threading.Barrier(2, timeout=5)
        threads: set[str] = set()

        def _get(key: EndpointKey) -> BytesLike | None:
            if key.endpoint_id != peer:
                return get(key)
            barrier.wait()
            threads.add(threading.current_thread().name)
            return key.object_id.encode()

        with mock.patch.object(conn, 'get', side_effect=_get):
            objs = conn.get_batch([peer_keys[0], local, peer_keys[1]])

        assert objs == [b'peer-0', b'local', b'peer-1']
        assert len(threads) == 2
        assert all(
            name.startswith('proxystore-endpoint-batch') for name in threads
        )


def test_get_batch_peer_error(endpoint_connector) -> None:
    with EndpointConnector.from_config(endpoint_connector.config()) as conn:
        peer = EndpointId.random()
        keys = [EndpointKey(f'peer-{i}', peer) for i in range(3)]

        def _get(key: EndpointKey) -> bytes:
            if key.object_id == 'peer-1':
                raise EndpointConnectorError('failed')
            return b'value'

        with (
            mock.patch.object(conn, 'get', side_effect=_get),
            pytest.raises(EndpointConnectorError, match='failed'),
        ):
            conn.get_batch(keys)


def test_close_stops_batch_threads(endpoint_connector) -> None:
    conn = EndpointConnector.from_config(endpoint_connector.config())
    executor = conn._pool.executor()
    assert conn._pool.executor() is executor
    conn.close()
    assert conn._pool._executor is None
    with pytest.raises(RuntimeError, match='shutdown'):
        executor.submit(print)


def test_batch_threads_reset_after_fork(endpoint_connector) -> None:
    with EndpointConnector.from_config(endpoint_connector.config()) as conn:
        executor = conn._pool.executor()
        conn._pool._reset_after_fork()
        assert conn._pool._executor is None
        # This process was not actually forked so stop the old threads.
        executor.shutdown()
