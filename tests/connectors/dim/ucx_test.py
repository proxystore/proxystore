"""UCXConnector Unit Tests."""
from __future__ import annotations

import asyncio
import importlib
from unittest import mock

import pytest

from proxystore.connectors.dim.exceptions import ServerTimeoutError
from proxystore.connectors.dim.rpc import RPC
from proxystore.connectors.dim.rpc import RPCResponse
from proxystore.connectors.dim.ucx import run_server
from proxystore.connectors.dim.ucx import spawn_server
from proxystore.connectors.dim.ucx import UCXConnector
from proxystore.connectors.dim.ucx import UCXServer
from proxystore.serialize import deserialize
from proxystore.serialize import serialize
from testing.mocking import mock_multiprocessing

ENCODING = 'UTF-8'

UCP_SPEC = importlib.util.find_spec('ucp')


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
            r = RPCResponse('exists', 'key', 0, exception=Exception('test'))
            return serialize(r)

    with mock.patch(
        'proxystore.connectors.dim.ucx.wait_for_server',
    ), mock.patch('ucp.create_endpoint', return_value=MockEndpoint()):
        with UCXConnector('eth0', 0) as connector:
            with pytest.raises(Exception, match='test'):
                connector._send_rpcs([RPC('get', 'key', 0)])


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

    rpc = RPC('put', 'key', 0, data=b'data')
    response = server.handle_rpc(rpc)
    assert response.exception is None

    rpc = RPC('exists', 'key', 0)
    response = server.handle_rpc(rpc)
    assert response.exists

    rpc = RPC('get', 'key', 0)
    response = server.handle_rpc(rpc)
    assert response.data == b'data'

    rpc = RPC('evict', 'key', 0)
    response = server.handle_rpc(rpc)
    assert response.exception is None

    rpc = RPC('get', 'key', 0)
    response = server.handle_rpc(rpc)
    assert response.data is None

    rpc = RPC('exists', 'key', 0)
    response = server.handle_rpc(rpc)
    assert not response.exists


def test_server_handle_rpc_exception() -> None:
    server = UCXServer()

    rpc = RPC('exists', 'key', 0)
    with mock.patch.object(server, 'exists', side_effect=Exception('test')):
        response = server.handle_rpc(rpc)
    assert response.exception is not None


@pytest.mark.asyncio()
async def test_server_handler() -> None:
    server = UCXServer()

    rpc = RPC('exists', 'key', 0)

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


@pytest.mark.asyncio()
async def test_run_server() -> None:
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    future.set_result(None)

    with mock.patch.object(loop, 'add_signal_handler'), mock.patch.object(
        loop,
        'create_future',
        return_value=future,
    ):
        await run_server(0)


def test_mocked_spawn_server() -> None:
    with mock_multiprocessing(), mock.patch(
        'proxystore.connectors.dim.ucx.wait_for_server',
    ), mock.patch('atexit.register') as mock_register:
        spawn_server('localhost', 0)
        mock_register.assert_called_once()
