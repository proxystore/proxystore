"""Utility functions."""
from __future__ import annotations

import random
from typing import Any
from typing import Generator


def chunk_bytes(
    data: bytes,
    chunk_size: int,
) -> Generator[bytes, None, None]:
    """Yield chunks of binary data.

    Args:
        data (bytes): data to be chunked.
        chunk_size (int): chunk size in bytes.

    Returns:
        Generator that yields chunks of bytes.
    """
    length = len(data)
    for index in range(0, length, chunk_size):
        yield data[index : min(index + chunk_size, length)]


def create_key(obj: Any) -> str:
    """Generate key for the object.

    .. todo::

       * generate key based on object hash (Re: Issue #4)
       * consider how to deal with key collisions

    Args:
        obj: object to create key for

    Returns:
        random 128 bit string.
    """
    return str(random.getrandbits(128))


def fullname(obj: Any) -> str:
    """Return full name of object."""
    if hasattr(obj, '__module__'):
        module = obj.__module__
    else:
        module = obj.__class__.__module__
    if hasattr(obj, '__name__'):
        name = obj.__name__
    else:
        name = obj.__class__.__name__
    if module is None or module == str.__module__:
        return name
    return f'{module}.{name}'
