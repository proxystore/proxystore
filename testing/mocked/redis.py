"""Mocked classes for Redis."""
from __future__ import annotations

from typing import Any


class MockStrictRedis:
    """Mock StrictRedis."""

    def __init__(self, data: dict[str, Any], *args, **kwargs):
        self.data = data

    def close(self) -> None:
        """Close the client."""
        pass

    def delete(self, key: str) -> None:
        """Delete key."""
        if key in self.data:
            del self.data[key]

    def exists(self, key: str) -> bool:
        """Check if key exists."""
        return key in self.data

    def flushdb(self) -> None:
        """Remove all keys."""
        self.data.clear()

    def get(self, key: str) -> bytes | None:
        """Get value with key."""
        if key in self.data:
            return self.data[key]
        return None

    def mget(self, keys: list[str]) -> list[bytes | None]:
        """Get list of values from keys."""
        return [self.data.get(key, None) for key in keys]

    def mset(self, values: dict[str, bytes]) -> None:
        """Set list of values."""
        for key, value in values.items():
            self.set(key, value)

    def set(self, key: str, value: bytes) -> None:
        """Set value in MockStrictRedis."""
        self.data[key] = value
