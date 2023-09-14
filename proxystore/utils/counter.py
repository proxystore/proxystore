"""Atomic counting utilities."""
from __future__ import annotations

import threading


class AtomicCounter:
    """Thread-safe counter.

    Args:
        size: Optional max count upon which an exception will be raised.
    """

    def __init__(self, size: int | None = None) -> None:
        self._size = size
        self._value = 0
        self._lock = threading.Lock()

    def increment(self) -> int:
        """Get current count and increment value.

        Returns:
            Current count.

        Raises:
            ValueError: If current count is equal to or greater than size.
        """
        with self._lock:
            value = self._value
            if self._size is not None and value >= self._size:
                raise ValueError(f'Max counter size exceeded ({self._size}).')
            self._value += 1
            return value
