"""MargoConnector Unit Tests."""
from __future__ import annotations

import importlib
import importlib.util
import platform
from typing import Any
from unittest import mock

import pytest

from proxystore.connectors.dim.exceptions import ServerTimeoutError
from proxystore.connectors.dim.margo import _when_finalize
from proxystore.connectors.dim.margo import MargoConnector
from proxystore.connectors.dim.margo import MargoServer
from proxystore.connectors.dim.margo import spawn_server
from proxystore.connectors.dim.margo import start_server
from proxystore.connectors.dim.margo import wait_for_server
from proxystore.connectors.dim.models import DIMKey
from proxystore.connectors.dim.models import RPC
from proxystore.connectors.dim.models import RPCResponse
from proxystore.serialize import deserialize
from proxystore.serialize import serialize
from testing.mocked.pymargo import Bulk as MockBulk
from testing.mocked.pymargo import Engine as MockEngine
from testing.mocked.pymargo import Handle as MockHandle
from testing.mocking import mock_multiprocessing
from testing.utils import open_port

TIMEOUT = 0.5
TEST_KEY = DIMKey(
    'margo',
    obj_id='key',
    size=0,
    peer_host='localhost',
    peer_port=0,
)
MARGO_SPEC = importlib.util.find_spec('pymargo')


def test_connector_spawns_server() -> None:
    with mock.patch(
        'proxystore.connectors.dim.margo.wait_for_server',
        side_effect=ServerTimeoutError,
    ), mock.patch(
        'proxystore.connectors.dim.margo.Engine.lookup',
    ), mock.patch(
        'proxystore.connectors.dim.margo.Engine.addr',
        return_value='tcp://127.0.0.1:0',
    ), mock.patch(
        'proxystore.connectors.dim.margo.spawn_server',
    ) as mock_spawn_server:
        with MargoConnector(protocol='tcp', port=0):
            pass
        mock_spawn_server.assert_called_once()


@pytest.mark.skipif(
    MARGO_SPEC is not None and 'mock' in MARGO_SPEC.name,
    reason='Not compatible with mocked Margo module.',
)
def test_multiple_connectors() -> None:  # pragma: no cover
    port = open_port()
    # C1 creates the server
    c1 = MargoConnector(
        protocol='tcp',
        port=port,
        timeout=TIMEOUT,
        force_spawn_server=True,
    )
    c2 = MargoConnector(
        protocol='tcp',
        port=port,
        timeout=TIMEOUT,
    )

    key = c1.put(b'data')
    assert c2.get(key) == b'data'

    # C2 did not create the server so closing should not kill it
    c2.close()
    assert c1.get(key) == b'data'

    # C1 will actually stop the server
    c1.close()


def test_finalize() -> None:
    # Testing only for coverage
    _when_finalize()


def test_handle_server_error_responses() -> None:
    rpc = RPC('exists', TEST_KEY)
    response = RPCResponse(
        'exists',
        key=TEST_KEY,
        exception=RuntimeError('xyz'),
    )

    class _CallableRemoteFunction:
        def __call__(self, *arg: Any, **kwargs: Any) -> Any:
            return serialize(response)

    # Only testing client side behavior so mock as successful connection
    # to an existing server
    with mock.patch(
        'proxystore.connectors.dim.margo.wait_for_server',
    ), mock.patch('proxystore.connectors.dim.margo.Engine.lookup'), mock.patch(
        'proxystore.connectors.dim.margo.Engine.addr',
        return_value='tcp://127.0.0.1:0',
    ):
        connector = MargoConnector(
            protocol='tcp',
            port=0,
        )

    with mock.patch.object(
        connector._rpcs['exists'],
        'on',
        return_value=_CallableRemoteFunction(),
    ), mock.patch.object(connector.engine, 'lookup'):
        with pytest.raises(RuntimeError, match='xyz'):
            connector._send_rpcs([rpc])

    connector.close()


def test_mocked_margo_server() -> None:
    host = '127.0.0.1'
    port = open_port()
    margo_server = MargoServer(MockEngine(f'tcp://{host}:{port}'))

    val = bytearray(serialize('world'))
    size = len(val)

    bulk_str = MockBulk(val)
    h = MockHandle()

    margo_server.put(h, bulk_str, size, TEST_KEY)
    assert margo_server.data[TEST_KEY.obj_id] == bytearray(val)

    with pytest.raises(ValueError):
        margo_server.put(h, bulk_str, -1, TEST_KEY)

    dne_key = DIMKey(
        'margo',
        obj_id='test',
        size=0,
        peer_host='localhost',
        peer_port=0,
    )

    local_buff = bytearray(size)
    bulk_str = MockBulk(local_buff)
    margo_server.get(h, bulk_str, size, TEST_KEY)
    assert bulk_str.data == val

    local_buff = bytearray(size)
    bulk_str = MockBulk(local_buff)
    assert deserialize(h.response).exists
    margo_server.get(h, bulk_str, size, dne_key)
    assert not deserialize(h.response).exists

    local_buff = bytearray(1)
    bulk_str = MockBulk(local_buff)
    margo_server.exists(h, bulk_str, size, TEST_KEY)
    assert deserialize(h.response).exists

    margo_server.exists(h, bulk_str, size, dne_key)
    assert not deserialize(h.response).exists

    local_buff = bytearray(1)
    bulk_str = MockBulk(local_buff)
    margo_server.evict(h, bulk_str, size, TEST_KEY)
    assert deserialize(h.response).exception is None
    assert TEST_KEY.obj_id not in margo_server.data

    local_buff = bytearray(1)
    bulk_str = MockBulk(local_buff)
    margo_server.evict(h, bulk_str, size, TEST_KEY)
    assert deserialize(h.response).exception is None


@pytest.mark.skipif(
    MARGO_SPEC is not None and 'mock' not in MARGO_SPEC.name,
    reason='Only compatible with mocked Margo module.',
)
def test_mocked_spawn_server() -> None:
    with mock_multiprocessing(), mock.patch(
        'proxystore.connectors.dim.margo.wait_for_server',
    ), mock.patch('atexit.register') as mock_register:
        spawn_server('tcp', '127.0.0.1', 0)
        mock_register.assert_called_once()

    # start_server is not called because we mock multiprocessing.Process
    start_server('tcp://127.0.0.1:0')


@pytest.mark.skipif(
    MARGO_SPEC is not None and 'mock' not in MARGO_SPEC.name,
    reason='Only compatible with mocked Margo module.',
)
def test_wait_for_server_raises_error() -> None:
    with pytest.raises(ServerTimeoutError):
        wait_for_server('tcp', '127.0.0.1', 0, timeout=0)


def test_provide_ip() -> None:
    with mock.patch(
        'proxystore.connectors.dim.margo.wait_for_server',
    ):
        with MargoConnector(
            port=0,
            protocol='tcp',
            address='127.0.0.1',
        ) as connector:
            assert connector.url == 'tcp://127.0.0.1:0'


@pytest.mark.skipif(
    platform.system() == 'Darwin',
    reason=(
        'Resolving an IP address from an interface is not supported on MacOS'
    ),
)
def test_provide_interface() -> None:  # pragma: darwin no cover
    with mock.patch(
        'proxystore.connectors.dim.margo.wait_for_server',
    ):
        with MargoConnector(
            port=0,
            protocol='tcp',
            interface='lo',
        ) as connector:
            assert connector.url == 'tcp://127.0.0.1:0'
