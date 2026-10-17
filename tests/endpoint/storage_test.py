from __future__ import annotations

import asyncio
import pathlib
import sqlite3
from collections.abc import AsyncGenerator
from unittest import mock

import aiosqlite
import pytest
import pytest_asyncio

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
