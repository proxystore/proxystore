"""RedisStore Unit Tests."""
from __future__ import annotations

import pytest

from proxystore.store.dim.margo import MargoServer
from proxystore.store.dim.margo import MargoStore
from proxystore.store.dim.margo import when_finalize
from testing.mocker_modules.pymargo_mocker import Bulk
from testing.mocker_modules.pymargo_mocker import Engine
from testing.mocker_modules.pymargo_mocker import Handle

ENCODING = 'UTF-8'


@pytest.fixture
def margo_server():
    """Margo server fixture."""
    e = Engine('tcp://127.0.0.1:6000')
    yield MargoServer(e)


def test_margo_store(margo_store) -> None:
    """Test RedisStore.

    All MargoStore functionality should be covered in
    tests/store/store_*_test.py.
    """
    store = MargoStore(margo_store.name, **margo_store.kwargs)
    store._start_server()
    store.close()
    store.close()  # check that nothing happens


def test_margo_server(margo_server) -> None:
    key = 'hello'
    val = bytes('world', encoding=ENCODING)
    size = len(val)

    bulk_str = Bulk(val)
    h = Handle()

    margo_server.set(h, bulk_str, size, key)
    assert margo_server.data[key] == bytearray(val)

    margo_server.set(h, bulk_str, -1, key)
    assert not h.response.success

    local_buff = bytearray(size)
    bulk_str = Bulk(local_buff)
    margo_server.get(h, bulk_str, size, key)
    assert bulk_str.data == val

    local_buff = bytearray(size)
    bulk_str = Bulk(local_buff)
    assert h.response.success
    margo_server.get(h, bulk_str, size, 'test')
    assert not h.response.success

    local_buff = bytearray(1)
    bulk_str = Bulk(local_buff)
    margo_server.exists(h, bulk_str, size, key)
    assert bulk_str.data == bytes('1', encoding=ENCODING)

    margo_server.exists(h, bulk_str, size, 'test')
    assert bulk_str.data == bytes('0', encoding=ENCODING)

    local_buff = bytearray(2)
    bulk_str = Bulk(local_buff)
    margo_server.exists(h, bulk_str, -1, key)
    assert not h.response.success

    local_buff = bytearray(1)
    bulk_str = Bulk(local_buff)
    margo_server.evict(h, bulk_str, size, key)
    assert h.response.success
    assert key not in margo_server.data

    local_buff = bytearray(1)
    bulk_str = Bulk(local_buff)
    margo_server.evict(h, bulk_str, size, 'test')
    assert not h.response.success

    when_finalize()
