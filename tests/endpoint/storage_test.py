from __future__ import annotations

import asyncio
import pathlib
import sqlite3
from collections.abc import AsyncGenerator
from unittest import mock

import aiosqlite
import pytest
import pytest_asyncio

import proxystore.endpoint.storage as storage_module
from proxystore.endpoint.exceptions import StorageFullError
from proxystore.endpoint.storage import MemoryStorage
from proxystore.endpoint.storage import SQLiteStorage
from proxystore.endpoint.storage import Storage


@pytest_asyncio.fixture(params=['dict', 'sql'])
async def storage(request) -> AsyncGenerator[Storage, None]:
    s: Storage
    if request.param == 'dict':
        s = MemoryStorage()
    elif request.param == 'sql':
        s = SQLiteStorage(':memory:')
    else:
        raise AssertionError('Unreachable.')

    yield s

    await s.close()


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_sqlite_storage_concurrent_first_requests(
    tmp_path: pathlib.Path,
) -> None:
    storage = SQLiteStorage(tmp_path / 'blobs.db')
    with mock.patch(
        'aiosqlite.connect',
        wraps=aiosqlite.connect,
    ) as connect:
        await asyncio.gather(
            *(storage.set(f'key-{i}', b'value') for i in range(10)),
        )
    connect.assert_called_once()
    assert all(
        await asyncio.gather(*(storage.exists(f'key-{i}') for i in range(10))),
    )
    await storage.close()


async def test_sqlite_storage_close() -> None:
    storage = SQLiteStorage(':memory:')
    await storage.close()
    await storage.set('key', b'value')
    await storage.close()
    # Closing twice is a no-op
    await storage.close()


async def test_sqlite_storage_uses_wal(tmp_path: pathlib.Path) -> None:
    storage = SQLiteStorage(tmp_path / 'blobs.db')
    db = await storage.db()
    async with db.execute('PRAGMA journal_mode') as cursor:
        assert await cursor.fetchone() == ('wal',)
    # 1 is NORMAL.
    async with db.execute('PRAGMA synchronous') as cursor:
        assert await cursor.fetchone() == (1,)
    async with db.execute('PRAGMA locking_mode') as cursor:
        assert await cursor.fetchone() == ('exclusive',)
    await storage.set('key', b'value')
    # The WAL index is kept in memory rather than a shared memory file,
    # which does not work on network file systems.
    assert not (tmp_path / 'blobs.db-shm').exists()

    # Other connections cannot open the database while it is in use.
    with pytest.raises(sqlite3.OperationalError, match='locked'):
        sqlite3.connect(tmp_path / 'blobs.db', timeout=0).execute(
            'SELECT count(*) FROM blobs',
        )
    await storage.close()


async def test_sqlite_storage_existing_database_uses_wal(
    tmp_path: pathlib.Path,
) -> None:
    # A database made by an older version uses the default journal mode.
    path = tmp_path / 'blobs.db'
    async with aiosqlite.connect(path) as db:
        await db.execute(
            'CREATE TABLE blobs (key TEXT PRIMARY KEY, value BLOB NOT NULL)',
        )
        await db.execute("INSERT INTO blobs VALUES ('key', x'00')")
        await db.commit()

    storage = SQLiteStorage(path)
    assert await storage.get('key') == b'\x00'
    db = await storage.db()
    async with db.execute('PRAGMA journal_mode') as cursor:
        assert await cursor.fetchone() == ('wal',)
    await storage.close()


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


async def test_memory_storage_max_size() -> None:
    storage = MemoryStorage(max_size=10)
    await storage.set('a', b'12345')
    await storage.set('b', b'1234')
    assert storage.size == 9

    with pytest.raises(StorageFullError, match='exceed the maximum'):
        await storage.set('c', b'12')
    assert not await storage.exists('c')

    # Replacing a blob only counts the new size, and a replacement which
    # does not fit keeps the old blob.
    await storage.set('a', b'123456')
    assert storage.size == 10
    with pytest.raises(StorageFullError):
        await storage.set('a', b'1234567')
    assert await storage.get('a') == b'123456'

    await storage.evict('a')
    assert storage.size == 4
    await storage.set('c', b'123456')
    await storage.close()
    assert storage.size == 0


async def test_memory_storage_ttl() -> None:
    clock = _Clock()
    with mock.patch.object(storage_module, 'monotonic', clock):
        storage = MemoryStorage(ttl=10)
        await storage.set('a', b'a')
        clock.now += 5
        await storage.set('b', b'b')
        assert await storage.get('a') == b'a'

        clock.now += 5
        assert not await storage.exists('a')
        assert await storage.get('a') is None
        assert await storage.get('b') == b'b'

        # Storing a blob again restarts its time.
        await storage.set('b', b'b')
        clock.now += 9
        assert await storage.exists('b')
        clock.now += 1
        assert await storage.get('b') is None
        await storage.close()


async def test_memory_storage_expired_blobs_free_space() -> None:
    clock = _Clock()
    with mock.patch.object(storage_module, 'monotonic', clock):
        storage = MemoryStorage(max_size=4, ttl=10)
        await storage.set('a', b'12')
        clock.now += 5
        await storage.set('b', b'12')
        clock.now += 5
        # The expired blob is removed before checking the size limit.
        await storage.set('c', b'12')
        assert storage.size == 4
        assert await storage.exists('b')
        await storage.close()


async def test_sqlite_storage_ttl(tmp_path: pathlib.Path) -> None:
    clock = _Clock()
    with mock.patch.object(storage_module, 'time', clock):
        storage = SQLiteStorage(tmp_path / 'blobs.db', ttl=10)
        await storage.set('a', b'a')
        clock.now += 5
        await storage.set('b', b'b')
        clock.now += 5
        assert not await storage.exists('a')
        assert await storage.get('a') is None
        assert await storage.get('b') == b'b'
        await storage.close()


async def _count(path: pathlib.Path) -> int:
    async with (
        aiosqlite.connect(path) as db,
        db.execute('SELECT count(*) FROM blobs') as cursor,
    ):
        row = await cursor.fetchone()
    assert row is not None
    return row[0]


async def test_sqlite_storage_removes_expired_blobs(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / 'blobs.db'
    clock = _Clock()
    with mock.patch.object(storage_module, 'time', clock):
        storage = SQLiteStorage(path, ttl=10)
        await storage.set('a', b'a')
        clock.now += 10
        # Expired blobs are deleted at most once every ttl seconds, so this
        # set deletes the expired blob and the next one does not.
        await storage.set('b', b'b')
        clock.now += 9
        await storage.set('c', b'c')
        await storage.close()
        assert await _count(path) == 2


async def test_sqlite_storage_ttl_changed(tmp_path: pathlib.Path) -> None:
    path = tmp_path / 'blobs.db'
    clock = _Clock()
    with mock.patch.object(storage_module, 'time', clock):
        storage = SQLiteStorage(path)
        await storage.set('a', b'a')
        await storage.close()

        # Blobs stored without a ttl expire ttl seconds after the database
        # is opened with a ttl.
        storage = SQLiteStorage(path, ttl=10)
        clock.now += 9
        assert await storage.exists('a')
        await storage.close()

        # Opening without a ttl removes expiry times.
        storage = SQLiteStorage(path)
        clock.now += 100
        assert await storage.get('a') == b'a'
        await storage.close()


async def test_sqlite_storage_adds_expiry_column(
    tmp_path: pathlib.Path,
) -> None:
    # A database made by an older version has no expiry column.
    path = tmp_path / 'blobs.db'
    async with aiosqlite.connect(path) as db:
        await db.execute(
            'CREATE TABLE blobs (key TEXT PRIMARY KEY, value BLOB NOT NULL)',
        )
        await db.execute("INSERT INTO blobs VALUES ('key', x'00')")
        await db.commit()

    clock = _Clock()
    with mock.patch.object(storage_module, 'time', clock):
        storage = SQLiteStorage(path, ttl=10)
        assert await storage.get('key') == b'\x00'
        clock.now += 10
        assert await storage.get('key') is None
        await storage.close()
