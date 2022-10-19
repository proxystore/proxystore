"""Atomic counting utilities."""
from __future__ import annotations

import threading


class AtomicCounter:
    """Thread-safe counter."""

    def __init__(self, size: int | None = None) -> None:
        """Init AtomicCounter.

        Args:
            size (int): optional max count upon which an exception will be
                raised (default: None).
        """
        self._size = size
        self._value = 0
        self._lock = threading.Lock()

    def increment(self) -> int:
        """Get current count and increment value.

        Returns:
            current count (int).

        Raises:
            ValueError:
                if current count is equal to or greater than size.
        """
        with self._lock:
            value = self._value
            if self._size is not None and value >= self._size:
                raise ValueError(f'Max counter size exceeded ({self._size}).')
            self._value += 1
            return value
