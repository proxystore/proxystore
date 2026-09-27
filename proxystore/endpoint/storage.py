"""Storage of the objects in an endpoint.

The [`Storage`][proxystore.endpoint.storage.Storage] protocol is a simple
blob store. Storage implementations do not enforce limits on the size of
objects because the endpoint rejects objects which exceed its maximum object
size before they are read from the network.
"""

from __future__ import annotations

import asyncio
import pathlib
from typing import Protocol
from typing import runtime_checkable

import aiosqlite

from proxystore.endpoint.protocol import MessageData


@runtime_checkable
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
        """
        ...

    async def close(self) -> None:
        """Close the storage."""
        ...


class MemoryStorage:
    """Storage of blobs in memory.

    Blobs are lost when the storage is closed.
    """

    def __init__(self) -> None:
        self._data: dict[str, MessageData] = {}

    async def evict(self, key: str) -> None:
        """Evict a blob from storage.

        Args:
            key: Key associated with blob to evict.
        """
        self._data.pop(key, None)

    async def exists(self, key: str) -> bool:
        """Check if a blob exists in the storage.

        Args:
            key: Key associated with the blob to check.

        Returns:
            If a blob associated with the key exists.
        """
        return key in self._data

    async def get(self, key: str) -> MessageData | None:
        """Get a blob from storage.

        Args:
            key: Key associated with the blob to get.

        Returns:
            The blob associated with the key or `None` if it does not exist.
        """
        return self._data.get(key)

    async def set(self, key: str, blob: MessageData) -> None:
        """Store the blob associated with a key.

        Args:
            key: Key that will be used to retrieve the blob.
            blob: Blob to store.
        """
        self._data[key] = blob

    async def close(self) -> None:
        """Clear all stored blobs."""
        self._data.clear()


class SQLiteStorage:
    """Storage of blobs in a SQLite database.

    Args:
        database_path: Path to database file. `~` is expanded to the user's
            home directory. Use `":memory:"` for a database which is only
            stored in memory.
    """

    def __init__(
        self,
        database_path: str | pathlib.Path = ':memory:',
    ) -> None:
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
                await db.execute(
                    'CREATE TABLE IF NOT EXISTS blobs'
                    '(key TEXT PRIMARY KEY, value BLOB NOT NULL)',
                )
                self._db = db
            return self._db

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
            'SELECT count(*) FROM blobs WHERE key=?',
            (key,),
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
            'SELECT value FROM blobs WHERE key=?',
            (key,),
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
        await db.execute(
            'INSERT OR REPLACE INTO blobs (key, value) VALUES (?, ?)',
            (key, blob),
        )
        await db.commit()

    async def close(self) -> None:
        """Close the storage."""
        if self._db is not None:
            await self._db.close()
            self._db = None
