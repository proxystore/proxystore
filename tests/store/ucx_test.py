"""UCXStore Unit Tests."""
from __future__ import annotations

import asyncio
import pickle
from typing import Any

import pytest

from proxystore.store.dim.ucx import UCXServer
from proxystore.store.dim.ucx import UCXStore
from testing.mocker_modules.ucx_mocker import MockEndpoint

ENCODING = 'UTF-8'


@pytest.fixture
def ucx_server():
    yield UCXServer('127.0.0.1', 6000)


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
    store = UCXStore(ucx_store.name, **ucx_store.kwargs)
    store._start_server()


def test_ucx_server(ucx_server) -> None:
    """Test UCXServer."""
    key = 'hello'
    val = bytes('world', encoding=ENCODING)

    obj = pickle.dumps({'key': key, 'data': val, 'op': 'set'})
    ret = asyncio.run(execute_handler(obj, ucx_server))
    assert ret == bytes('1', encoding=ENCODING)
    assert ucx_server.data[key] == val

    data = ucx_server.get(key)
    assert data == val

    obj = pickle.dumps({'key': key, 'data': '', 'op': 'get'})
    ret = asyncio.run(execute_handler(obj, ucx_server))
    assert ret == val

    data = ucx_server.get('test')
    assert data == bytes('ERROR', encoding=ENCODING)

    ret = ucx_server.exists(key)
    assert ret == bytes('1', encoding=ENCODING)

    obj = pickle.dumps({'key': 'test', 'data': '', 'op': 'exists'})
    ret = asyncio.run(execute_handler(obj, ucx_server))
    assert ret == bytes('0', encoding=ENCODING)
    ret = ucx_server.exists('test')
    assert ret == bytes('0', encoding=ENCODING)

    obj = pickle.dumps({'key': key, 'data': '', 'op': 'evict'})
    ret = asyncio.run(execute_handler(obj, ucx_server))
    assert ret == bytes('1', encoding=ENCODING)

    ret = ucx_server.evict('test')
    assert ret == bytes('ERROR', encoding=ENCODING)
