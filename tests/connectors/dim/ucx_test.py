"""UCXConnector Unit Tests."""
from __future__ import annotations

import asyncio
import importlib
import sys
from typing import Any
from unittest import mock

if sys.version_info >= (3, 8):  # pragma: >=3.8 cover
    pass
else:  # pragma: <3.8 cover
    pass

import pytest

from proxystore.connectors.dim.ucx import run_server
from proxystore.connectors.dim.ucx import UCXConnector
from proxystore.connectors.dim.ucx import UCXServer
from proxystore.serialize import deserialize
from proxystore.serialize import serialize
from testing.mocked.ucx import MockEndpoint
from testing.mocking import mock_multiprocessing
from testing.utils import open_port

ENCODING = 'UTF-8'

UCP_SPEC = importlib.util.find_spec('ucp')


@pytest.fixture()
def ucx_server(ucx_connector):
    server = UCXServer()
    return server


async def execute_handler(obj: Any, server: UCXServer) -> Any:
    ep = MockEndpoint(server=True)
    await ep.send_obj(obj)
    await server.handler(ep)
    ret = await ep.recv_obj()
    return ret


def test_ucx_connector(ucx_connector) -> None:
    config = ucx_connector.config()
    config['port'] = open_port()
    with mock_multiprocessing():
        with UCXConnector.from_config(config) as connector:
            connector.close()
            connector.close()  # check that nothing happens


@pytest.mark.skipif(
    UCP_SPEC is not None and 'mock' not in UCP_SPEC.name,
    reason='only valid for running against the mocked ucp module',
)
def test_run_mocked_server() -> None:
    UCXServer()

    # We use this fake awaitable Future because we are not running in an
    # event loop so asyncio.create_future() will error.
    class _Future:
        def set_result(self, value: Any) -> None:
            ...

        def __await__(self) -> Any:
            yield
            return None

    mock_loop = mock.MagicMock()
    mock_loop.create_future = mock.MagicMock()
    mock_loop.create_future.return_value = _Future()

    with mock.patch('asyncio.get_running_loop', return_value=mock_loop):
        asyncio.run(run_server(open_port()))


def test_ucx_server(ucx_server) -> None:
    """Test UCXServer."""
    key = 'hello'
    val = bytes('world', encoding=ENCODING)

    # server_started call
    ret = asyncio.run(execute_handler(bytes(1), ucx_server))
    assert ret == bytes(1)

    obj = serialize({'key': key, 'data': val, 'op': 'set'})
    ret = deserialize(asyncio.run(execute_handler(obj, ucx_server)))
    assert ret.success
    assert ucx_server.data[key] == val

    data = ucx_server.get(key)
    assert data == val

    obj = serialize({'key': key, 'data': '', 'op': 'get'})
    ret = asyncio.run(execute_handler(obj, ucx_server))
    assert ret == val

    data = ucx_server.get('test')
    assert not data.success

    ret = ucx_server.exists(key)
    assert ret

    obj = serialize({'key': 'test', 'data': '', 'op': 'exists'})
    ret = deserialize(asyncio.run(execute_handler(obj, ucx_server)))
    assert not ret
    ret = ucx_server.exists('test')
    assert not ret

    obj = serialize({'key': key, 'data': '', 'op': 'evict'})
    ret = deserialize(asyncio.run(execute_handler(obj, ucx_server)))
    assert ret.success

    ret = ucx_server.evict('test')
    assert ret.success

    obj = serialize({'key': key, 'data': '', 'op': 'sum'})
    with pytest.raises(AssertionError):
        ret = asyncio.run(execute_handler(obj, ucx_server))
