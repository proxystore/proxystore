from __future__ import annotations

import pathlib
from typing import AsyncGenerator

import pytest
import pytest_asyncio

from proxystore.endpoint.exceptions import ObjectSizeExceededError
from proxystore.endpoint.storage import DictStorage
from proxystore.endpoint.storage import SQLiteStorage
from proxystore.endpoint.storage import Storage


@pytest_asyncio.fixture(params=['dict', 'sql'])
async def storage(request) -> AsyncGenerator[Storage, None]:
    s: Storage
    if request.param == 'dict':
        s = DictStorage()
    elif request.param == 'sql':
        s = SQLiteStorage(':memory:')
    else:
        raise AssertionError('Unreachable.')

    yield s

    await s.close()


@pytest.mark.asyncio()
async def test_storage_basics(storage: Storage) -> None:
    key = 'key'
    data = b'data'

    await storage.evict(key)
    assert not await storage.exists(key)
    assert await storage.get(key) is None

    await storage.set(key, data)
    assert await storage.exists(key)
    assert await storage.get(key) == data

    await storage.evict(key)
    assert not await storage.exists(key)
    assert await storage.get(key) is None
    assert await storage.get(key, b'123') == b'123'


@pytest.mark.asyncio()
async def test_sqlite_storage_persists(tmp_path: pathlib.Path) -> None:
    key = 'key'
    data = b'data'
    path = tmp_path / 'db.db'

    storage = SQLiteStorage(path)
    await storage.set(key, data)
    await storage.close()

    storage = SQLiteStorage(path)
    assert await storage.exists(key)
    await storage.close()


@pytest.mark.asyncio()
async def test_sqlite_storage_close() -> None:
    storage = SQLiteStorage(':memory:')
    await storage.close()


@pytest.mark.asyncio()
async def test_max_object_size_exceeded() -> None:
    dict_storage = DictStorage(max_object_size=100)
    with pytest.raises(ObjectSizeExceededError):
        await dict_storage.set('key', b'x' * 1000)

    sqlite_storage = SQLiteStorage(':memory:', max_object_size=100)
    with pytest.raises(ObjectSizeExceededError):
        await sqlite_storage.set('key', b'x' * 1000)
