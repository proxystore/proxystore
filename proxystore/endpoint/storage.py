"""Blob storage interface for endpoints."""
from __future__ import annotations

import pathlib
import sys

if sys.version_info >= (3, 8):  # pragma: >=3.8 cover
    from typing import Protocol
    from typing import runtime_checkable
else:  # pragma: <3.8 cover
    from typing_extensions import Protocol
    from typing_extensions import runtime_checkable

import aiosqlite


@runtime_checkable
class Storage(Protocol):
    """Endpoint storage protocol for blobs."""

    async def evict(self, key: str) -> None:
        """Evict a blob from storage.

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

    async def get(
        self,
        key: str,
        default: bytes | None = None,
    ) -> bytes | None:
        """Get a blob from storage.

        Args:
            key: Key associated with the blob to get.
            default: Default return value if the blob does not exist.

        Returns:
            The blob associated with the key or the value of `default`.
        """
        ...

    async def set(self, key: str, blob: bytes) -> None:
        """Store the blob associated with a key.

        Args:
            key: Key that will be used to retrieve the blob.
            blob: Blob to store.
        """
        ...

    async def close(self) -> None:
        """Close the storage."""
        ...


class DictStorage:
    """Simple dictionary-based storage for blobs."""

    def __init__(self) -> None:
        self._data: dict[str, bytes] = {}

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

    async def get(
        self,
        key: str,
        default: bytes | None = None,
    ) -> bytes | None:
        """Get a blob from storage.

        Args:
            key: Key associated with the blob to get.
            default: Default return value if the blob does not exist.

        Returns:
            The blob associated with the key or the value of `default`.
        """
        return self._data.get(key, default)

    async def set(self, key: str, blob: bytes) -> None:
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
    """SQLite storage protocol for blobs."""

    def __init__(self, database_path: str | pathlib.Path = ':memory:') -> None:
        if database_path == ':memory:':
            self.database_path = database_path
        else:
            path = pathlib.Path(database_path).expanduser().resolve()
            self.database_path = str(path)

        self._db: aiosqlite.Connection | None = None

    async def db(self) -> aiosqlite.Connection:
        """Get the database connection object."""
        if self._db is None:
            self._db = await aiosqlite.connect(self.database_path)
            await self._db.execute(
                'CREATE TABLE IF NOT EXISTS blobs'
                '(key TEXT PRIMARY KEY, value BLOB NOT NULL)',
            )
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
            (count,) = await cursor.fetchone()
            return bool(count)

    async def get(
        self,
        key: str,
        default: bytes | None = None,
    ) -> bytes | None:
        """Get a blob from storage.

        Args:
            key: Key associated with the blob to get.
            default: Default return value if the blob does not exist.

        Returns:
            The blob associated with the key or the value of `default`.
        """
        db = await self.db()
        async with db.execute(
            'SELECT value FROM blobs WHERE key=?',
            (key,),
        ) as cursor:
            result = await cursor.fetchone()
            if result is None:
                return default
            else:
                return result[0]

    async def set(self, key: str, blob: bytes) -> None:
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
