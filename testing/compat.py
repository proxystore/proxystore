"""Utilities for supporting across python version."""

from __future__ import annotations

import random


def randbytes(size: int) -> bytes:
    """Get random byte string of specified size.

    This function exists for legacy reasons: `random.randbytes()` did not
    exist in Python 3.8 and older.

    Args:
        size: The size of byte string to return.

    Returns:
        A random byte string.
    """
    return random.randbytes(size)
