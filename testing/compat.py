"""Utilities for supporting across python version."""
from __future__ import annotations

import os
import random
import sys


def randbytes(size: int) -> bytes:
    """Get random byte string of specified size.

    Uses `random.randbytes()` in Python 3.9 or newer and
    `os.urandom()` in Python 3.8 and older.

    Args:
        size: The size of byte string to return.

    Returns:
        A random byte string.
    """
    if sys.version_info >= (3, 9):  # pragma: >=3.9 cover
        return random.randbytes(size)
    else:  # pragma: <3.9 cover
        return os.urandom(size)
