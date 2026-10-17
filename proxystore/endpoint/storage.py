"""Storage of the objects in an endpoint.

The [`Storage`][proxystore.endpoint.storage.Storage] protocol is a simple
blob store. Storage implementations do not enforce limits on the size of
objects because the endpoint rejects objects which exceed its maximum object
size before they are read from the network. They may limit the total size of
the stored blobs and remove blobs a fixed time after they are stored.
"""

from __future__ import annotations

import asyncio
import pathlib
from time import monotonic
from time import time
from typing import Protocol

import aiosqlite

from proxystore.endpoint.exceptions import StorageFullError
from proxystore.endpoint.protocol import MessageData

_MAX_SWEEP_INTERVAL = 60
"""Maximum seconds between removing the expired blobs of SQLite storage."""


class Storage(Protocol):
    """Storage of the objects in an endpoint."""

    async def evict(self, key: str) -> None:
        """Evict a blob from storage.

        Evicting a key which does not exist is not an error.

        Args:
            key: Key associated with blob to evict.
        """
        ...

    async def exists(self, key: str) -> bool:
        """Check if a blob exists in the storage.

        Args:
            key: Key associated with the blob to check.

        Returns:
            If a blob associated with the key exists.
        """
        ...

    async def get(self, key: str) -> MessageData | None:
        """Get a blob from storage.

        Args:
            key: Key associated with the blob to get.

        Returns:
            The blob associated with the key or `None` if it does not exist.
        """
        ...

    async def set(self, key: str, blob: MessageData) -> None:
        """Store the blob associated with a key.

        Args:
            key: Key that will be used to retrieve the blob.
            blob: Blob to store.

        Raises:
            StorageFullError: If the storage has a size limit and the blob
                does not fit.
        """
        ...

    async def close(self) -> None:
        """Close the storage."""
        ...


class MemoryStorage:
    """Storage of blobs in memory.

    Blobs are lost when the storage is closed.

    Args:
        max_size: Maximum total size in bytes of the stored blobs. Storing a
            blob which does not fit raises a
            [`StorageFullError`][proxystore.endpoint.exceptions.StorageFullError]
            rather than removing other blobs. If `None`, there is no limit.
        ttl: Seconds after a blob is stored that it expires and is removed.
            If `None`, blobs do not expire.
    """

    def __init__(
        self,
        max_size: int | None = None,
        ttl: float | None = None,
    ) -> None:
        self.max_size = max_size
        self.ttl = ttl
        self._data: dict[str, MessageData] = {}
        self._size = 0
        # Expiry time of each blob, from a clock which is not changed by
        # updates to the system time. Blobs are moved to the end when stored
        # so the dict is ordered by expiry time and expired blobs are at the
        # start.
        self._expires: dict[str, float] = {}

    @property
    def size(self) -> int:
        """Total size in bytes of the stored blobs."""
        return self._size

    def _remove(self, key: str) -> None:
        blob = self._data.pop(key, None)
        if blob is not None:
            self._size -= len(blob)
        self._expires.pop(key, None)

    def _remove_expired(self) -> None:
        # Only the expired blobs at the start of the dict are visited.
        now = monotonic()
        while self._expires:
            key, expires = next(iter(self._expires.items()))
            if expires > now:
                break
            self._remove(key)

    def _expired(self, key: str) -> bool:
        expires = self._expires.get(key)
        if expires is not None and expires <= monotonic():
            self._remove(key)
            return True
        return False

    async def evict(self, key: str) -> None:
        """Evict a blob from storage.

        Args:
            key: Key associated with blob to evict.
        """
        self._remove(key)

    async def exists(self, key: str) -> bool:
        """Check if a blob exists in the storage.

        Args:
            key: Key associated with the blob to check.

        Returns:
            If a blob associated with the key exists.
        """
        return not self._expired(key) and key in self._data

    async def get(self, key: str) -> MessageData | None:
        """Get a blob from storage.

        Args:
            key: Key associated with the blob to get.

        Returns:
            The blob associated with the key or `None` if it does not exist.
        """
        if self._expired(key):
            return None
        return self._data.get(key)

    async def set(self, key: str, blob: MessageData) -> None:
        """Store the blob associated with a key.

        Args:
            key: Key that will be used to retrieve the blob.
            blob: Blob to store.

        Raises:
            StorageFullError: If the blob does not fit within `max_size`.
                A blob already stored with the key is kept.
        """
        self._remove_expired()
        if self.max_size is not None:
            old = self._data.get(key)
            size = self._size - (0 if old is None else len(old)) + len(blob)
            if size > self.max_size:
                raise StorageFullError(
                    f'Storing {len(blob)} bytes would exceed the maximum '
                    f'storage size of the endpoint ({self.max_size} bytes, '
                    f'{self._size} bytes used).',
                )
        self._remove(key)
        self._data[key] = blob
        self._size += len(blob)
        if self.ttl is not None:
            self._expires[key] = monotonic() + self.ttl

    async def close(self) -> None:
        """Clear all stored blobs."""
        self._data.clear()
        self._expires.clear()
        self._size = 0


class SQLiteStorage:
    """Storage of blobs in a SQLite database.

    The database uses write-ahead logging with `synchronous=NORMAL`, so
    commits are not synced to disk one at a time. The database is never
    corrupted, and a crash of the endpoint loses no blobs, but a crash of
    the operating system or a power loss can lose the most recently stored
    blobs. The database is locked while the storage is open, so other
    processes cannot open it.

    Args:
        database_path: Path to database file. `~` is expanded to the user's
            home directory. Use `":memory:"` for a database which is only
            stored in memory.
        ttl: Seconds after a blob is stored that it expires and is removed.
            When the database is opened, blobs without an expiry time expire
            `ttl` seconds later. If `None`, blobs do not expire, and expiry
            times of blobs already in the database are removed.
    """

    def __init__(
        self,
        database_path: str | pathlib.Path = ':memory:',
        ttl: float | None = None,
    ) -> None:
        self.ttl = ttl
        self._next_sweep = 0.0
        if database_path == ':memory:':
            self.database_path = database_path
        else:
            path = pathlib.Path(database_path).expanduser().resolve()
            self.database_path = str(path)

        self._db: aiosqlite.Connection | None = None
        self._db_lock = asyncio.Lock()

    async def db(self) -> aiosqlite.Connection:
        """Get the database connection object."""
        if self._db is not None:
            return self._db
        # Concurrent first requests (e.g., when clients reconnect after the
        # endpoint restarts) must share one connection. Separate connections
        # can deadlock each other's write transactions.
        async with self._db_lock:
            if self._db is None:
                db = await aiosqlite.connect(self.database_path)
                # Each set() and evict() is its own transaction, and syncing
                # each one to disk made writes about 16x slower. The journal
                # mode is kept in the database file, so existing databases
                # switch to WAL when opened. A database in memory has no
                # journal file, so it keeps its "memory" journal mode.
                #
                # Only this connection uses the database, so it is locked
                # exclusively. This must be set before WAL so SQLite keeps
                # the WAL index in memory instead of a shared memory file,
                # which does not work on network file systems.
                await db.execute('PRAGMA locking_mode=EXCLUSIVE')
                await db.execute('PRAGMA journal_mode=WAL')
                await db.execute('PRAGMA synchronous=NORMAL')
                await db.execute(
                    'CREATE TABLE IF NOT EXISTS blobs'
                    '(key TEXT PRIMARY KEY, value BLOB NOT NULL, '
                    'expires_at REAL)',
                )
                await _add_expires_column(db)
                await db.execute(
                    'CREATE INDEX IF NOT EXISTS blobs_expires_at '
                    'ON blobs (expires_at)',
                )
                if self.ttl is None:
                    await db.execute(
                        'UPDATE blobs SET expires_at = NULL '
                        'WHERE expires_at IS NOT NULL',
                    )
                else:
                    await db.execute(
                        'UPDATE blobs SET expires_at = ? '
                        'WHERE expires_at IS NULL',
                        (time() + self.ttl,),
                    )
                await db.commit()
                self._db = db
            return self._db

    async def _remove_expired(self, db: aiosqlite.Connection) -> None:
        # Expired blobs are never returned, so they are only deleted
        # occasionally rather than with every write. Expiry times use the
        # system time because they are kept in the database across restarts.
        assert self.ttl is not None
        now = time()
        if now < self._next_sweep:
            return
        self._next_sweep = now + min(self.ttl, _MAX_SWEEP_INTERVAL)
        await db.execute('DELETE FROM blobs WHERE expires_at <= ?', (now,))

    async def evict(self, key: str) -> None:
        """Evict a blob from storage.

        Args:
            key: Key associated with blob to evict.
        """
        db = await self.db()
        await db.execute('DELETE FROM blobs WHERE key=?', (key,))
        await db.commit()

    async def exists(self, key: str) -> bool:
        """Check if a blob exists in the storage.

        Args:
            key: Key associated with the blob to check.

        Returns:
            If a blob associated with the key exists.
        """
        db = await self.db()
        async with db.execute(
            'SELECT count(*) FROM blobs WHERE key=? '
            'AND (expires_at IS NULL OR expires_at > ?)',
            (key, time()),
        ) as cursor:
            result = await cursor.fetchone()
            # count() won't ever return 0 rows but mypy doesn't know this
            assert result is not None
            (count,) = result
            return bool(count)

    async def get(self, key: str) -> MessageData | None:
        """Get a blob from storage.

        Args:
            key: Key associated with the blob to get.

        Returns:
            The blob associated with the key or `None` if it does not exist.
        """
        db = await self.db()
        async with db.execute(
            'SELECT value FROM blobs WHERE key=? '
            'AND (expires_at IS NULL OR expires_at > ?)',
            (key, time()),
        ) as cursor:
            result = await cursor.fetchone()
            if result is None:
                return None
            return result[0]

    async def set(self, key: str, blob: MessageData) -> None:
        """Store the blob associated with a key.

        Args:
            key: Key that will be used to retrieve the blob.
            blob: Blob to store.
        """
        db = await self.db()
        expires_at = None
        if self.ttl is not None:
            await self._remove_expired(db)
            expires_at = time() + self.ttl
        await db.execute(
            'INSERT OR REPLACE INTO blobs (key, value, expires_at) '
            'VALUES (?, ?, ?)',
            (key, blob, expires_at),
        )
        await db.commit()

    async def close(self) -> None:
        """Close the storage."""
        if self._db is not None:
            await self._db.close()
            self._db = None


async def _add_expires_column(db: aiosqlite.Connection) -> None:
    # Databases made before blobs could expire do not have the column.
    async with db.execute('PRAGMA table_info(blobs)') as cursor:
        columns = {row[1] for row in await cursor.fetchall()}
    if 'expires_at' not in columns:
        await db.execute('ALTER TABLE blobs ADD COLUMN expires_at REAL')
