"""Blob storage interface for endpoints."""
from __future__ import annotations

from typing import Protocol
from typing import runtime_checkable


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
        return self._data.get(key, None)

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
