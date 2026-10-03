"""Factory implementations for testing."""

from __future__ import annotations

from typing import Generic
from typing import TypeVar

T = TypeVar('T')


class SimpleFactory(Generic[T]):
    """Factory that returns the object it was initialized with.

    Args:
        obj: Object to produce when factory is called.
    """

    def __init__(self, obj: T) -> None:
        self._obj = obj

    def __call__(self) -> T:
        """Return the object."""
        return self._obj
