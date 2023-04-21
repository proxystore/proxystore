"""UCXConnector Unit Tests."""
from __future__ import annotations

import asyncio
import importlib
import sys
from unittest import mock

if sys.version_info >= (3, 8):  # pragma: >=3.8 cover
    from unittest.mock import AsyncMock
else:  # pragma: <3.8 cover
    from asynctest import CoroutineMock as AsyncMock

import pytest

from proxystore.connectors.dim.exceptions import ServerTimeoutError
from proxystore.connectors.dim.models import DIMKey
from proxystore.connectors.dim.models import RPC
from proxystore.connectors.dim.models import RPCResponse
from proxystore.connectors.dim.ucx import run_server
from proxystore.connectors.dim.ucx import spawn_server
from proxystore.connectors.dim.ucx import UCXConnector
from proxystore.connectors.dim.ucx import UCXServer
from proxystore.serialize import deserialize
from proxystore.serialize import serialize
from testing.mocking import mock_multiprocessing
from testing.utils import open_port

ENCODING = 'UTF-8'

UCP_SPEC = importlib.util.find_spec('ucp')
TEST_KEY = DIMKey(
    'ucx',
    obj_id='key',
    size=0,
    peer_host='localhost',
    peer_port=0,
)


@pytest.mark.asyncio()
async def test_connector_spawns_server() -> None:
    with mock.patch(
        'proxystore.connectors.dim.ucx.wait_for_server',
        side_effect=ServerTimeoutError,
    ), mock.patch(
        'proxystore.connectors.dim.ucx.spawn_server',
    ) as mock_spawn_server:
        with UCXConnector('eth0', 0):
            pass
        mock_spawn_server.assert_called_once()


def test_connector_raises_rpc_error() -> None:
    class MockEndpoint:
        async def send_obj(self, data: bytes) -> None:
            pass

        async def recv_obj(self) -> bytes:
            r = RPCResponse('exists', TEST_KEY, exception=Exception('test'))
            return serialize(r)

    with mock.patch(
        'proxystore.connectors.dim.ucx.wait_for_server',
    ), mock.patch(
        'ucp.create_endpoint',
        AsyncMock(return_value=MockEndpoint()),
    ):
        with UCXConnector('eth0', 0) as connector:
            with pytest.raises(Exception, match='test'):
                connector._send_rpcs([RPC('get', TEST_KEY)])


def test_connector_close_kills_server() -> None:
    class MockProcess:
        terminate_called = False
        join_called = False
        pid = 0

        def terminate(self) -> None:
            self.terminate_called = True

        def join(self) -> None:
            self.join_called = True

    with mock.patch('proxystore.connectors.dim.ucx.wait_for_server'):
        connector = UCXConnector('eth0', 0)

    connector.server = MockProcess()  # type: ignore[assignment]
    connector.close(kill_server=True)
    assert connector.server.terminate_called  # type: ignore[union-attr]
    assert connector.server.join_called  # type: ignore[union-attr]


def test_server_handle_rpc() -> None:
    server = UCXServer()

    rpc = RPC('put', TEST_KEY, data=b'data')
    response = server.handle_rpc(rpc)
    assert response.exception is None

    rpc = RPC('exists', TEST_KEY)
    response = server.handle_rpc(rpc)
    assert response.exists

    rpc = RPC('get', TEST_KEY)
    response = server.handle_rpc(rpc)
    assert response.data == b'data'

    rpc = RPC('evict', TEST_KEY)
    response = server.handle_rpc(rpc)
    assert response.exception is None

    rpc = RPC('get', TEST_KEY)
    response = server.handle_rpc(rpc)
    assert response.data is None

    rpc = RPC('exists', TEST_KEY)
    response = server.handle_rpc(rpc)
    assert not response.exists


def test_server_handle_rpc_exception() -> None:
    server = UCXServer()

    rpc = RPC('exists', TEST_KEY)
    with mock.patch.object(server, 'exists', side_effect=Exception('test')):
        response = server.handle_rpc(rpc)
    assert response.exception is not None


@pytest.mark.asyncio()
async def test_server_handler() -> None:
    server = UCXServer()

    rpc = RPC('exists', TEST_KEY)

    class MockEndpoint:
        received: bytes | None = None

        async def send_obj(self, data: bytes) -> None:
            self.received = data

        async def recv_obj(self) -> bytes:
            return serialize(rpc)

    ep = MockEndpoint()
    await server.handler(ep)
    assert ep.received is not None
    response = deserialize(ep.received)
    assert not response.exists
    assert response.exception is None


@pytest.mark.asyncio()
async def test_server_handler_ping() -> None:
    server = UCXServer()

    class MockEndpoint:
        received: bytes | None = None

        async def send_obj(self, data: bytes) -> None:
            self.received = data

        async def recv_obj(self) -> bytes:
            return b'ping'

    ep = MockEndpoint()
    await server.handler(ep)
    assert ep.received is not None
    assert ep.received == b'pong'


# NOTE: this test causes random hangs with Click's CLIRunner
@pytest.mark.skip()
@pytest.mark.asyncio()
async def test_run_server() -> None:  # pragma: no cover
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    future.set_result(None)

    with mock.patch.object(
        loop,
        'create_future',
        return_value=future,
    ), mock.patch(
        'proxystore.connectors.dim.ucx.reset_ucp_async',
        AsyncMock(),
    ):
        await run_server(0)


@pytest.mark.skipif(
    UCP_SPEC is not None and 'mock' not in UCP_SPEC.name,
    reason='Only compatible with mocked UCP module.',
)
def test_mocked_spawn_server() -> None:
    with mock_multiprocessing(), mock.patch(
        'proxystore.connectors.dim.ucx.wait_for_server',
    ), mock.patch('atexit.register') as mock_register:
        spawn_server('localhost', 0)
        mock_register.assert_called_once()


# This test will hang when run in the Docker image
@pytest.mark.skip()
@pytest.mark.skipif(
    UCP_SPEC is not None and 'mock' in UCP_SPEC.name,
    reason='Not compatible with mocked UCP module.',
)
def test_end_to_end() -> None:  # pragma: no cover
    host = '127.0.0.1'
    port = open_port()

    spawn_server(host, port, spawn_timeout=1)

    with UCXConnector(host, port, timeout=1) as connector:
        # assert connector.server is not None

        # Check multiple connectors okay
        connector2 = UCXConnector(host, port, timeout=1)
        connector2.close()
        assert connector2.server is None

        key = connector.put(b'data')
        assert connector.get(key) == b'data'
