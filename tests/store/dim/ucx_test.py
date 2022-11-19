"""UCXStore Unit Tests."""
from __future__ import annotations

import asyncio
from typing import Any

import pytest

from proxystore.serialize import deserialize
from proxystore.serialize import serialize
from proxystore.store.dim.ucx import UCXServer
from proxystore.store.dim.ucx import UCXStore
from testing.mocked.ucx import MockEndpoint
from testing.utils import open_port

ENCODING = 'UTF-8'


@pytest.fixture
def ucx_server():
    yield UCXServer('localhost', open_port())


async def execute_handler(obj: Any, server: UCXServer) -> Any:
    ep = MockEndpoint(server=True)
    await ep.send_obj(obj)
    await server.handler(ep)
    ret = await ep.recv_obj()
    return ret


def test_ucx_store(ucx_store) -> None:
    """Test UCXStore.

    All UCXStore functionality should be covered in
    tests/store/store_*_test.py.
    """
    with ucx_store.ctx():
        store = UCXStore(ucx_store.name, **ucx_store.kwargs)
        store._start_server()
        store.close()
        store.close()  # check that nothing happens


def test_ucx_server(ucx_server) -> None:
    """Test UCXServer."""
    key = 'hello'
    val = bytes('world', encoding=ENCODING)

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

    with pytest.raises(AssertionError):
        obj = serialize({'key': key, 'data': '', 'op': 'sum'})
        ret = asyncio.run(execute_handler(obj, ucx_server))
