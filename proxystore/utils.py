"""General purpose utility functions."""
from __future__ import annotations

import decimal
import os
import random
import re
import socket
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


def home_dir() -> str:
    """Return the absolute path to the proxystore home directory.

    If set, ``$PROXYSTORE_HOME`` is preferred. Otherwise,
    ``$XDG_DATA_HOME/proxystore`` is returned where ``$XDG_DATA_HOME`` defaults
    to ``$HOME/.local/share`` if unset.
    """
    path = os.environ.get('PROXYSTORE_HOME')
    if path is None:
        prefix = os.environ.get('XDG_DATA_HOME') or os.path.expanduser(
            '~/.local/share',
        )
        path = os.path.join(prefix, 'proxystore')
    return os.path.abspath(path)


def hostname() -> str:
    """Return current hostname."""
    return socket.gethostname()


def bytes_to_readable(size: int, precision: int = 3) -> str:
    """Convert bytes to human readable value.

    Note:
        This method uses base-10 values for KB, MB, GB, etc. instead of
        base-2 values (i.e., KiB, MiB, GiB, etc.).

    Args:
        size (int): byte count to make readable.
        precision (int): number of decimal places (default: 3).

    Returns:
        string with human readable number of bytes.

    Raises:
        ValueError:
            if size is negative.
    """
    kb = int(1e3)
    mb = int(1e6)
    gb = int(1e9)
    tb = int(1e12)

    size_ = float(size)
    if 0 <= size < kb:
        suffix = 'B'
    elif kb <= size < mb:
        suffix = 'KB'
        size_ /= kb
    elif mb <= size < gb:
        suffix = 'MB'
        size_ /= mb
    elif gb <= size < tb:
        suffix = 'GB'
        size_ /= gb
    elif tb <= size:
        suffix = 'TB'
        size_ /= tb
    else:
        raise ValueError(f'Size ({size}) cannot be negative.')

    value = str(round(size_, precision))
    value = value.rstrip('0').rstrip('.')
    return f'{value} {suffix}'


def readable_to_bytes(size: str) -> int:
    """Convert string with bytes units to the integer value of bytes.

    Example:
        >>> readable_to_bytes('1.2 KB')
        1200
        >>> readable_to_bytes('0.6 MiB')
        629146

    Args:
        size (str): string to parse for bytes size.

    Returns:
        integer number of bytes parsed from the string.

    Raises:
        ValueError:
            if the input string contains more than two parts (i.e., a value
            and a unit).
        ValueError:
            if the unit is not one of KB, MB, GB, TB, KiB, MiB, GiB, or TiB.
        ValueError:
            if the value cannot be cast to a float.
    """
    units_to_bytes = dict(
        b=1,
        kb=int(1e3),
        mb=int(1e6),
        gb=int(1e9),
        tb=int(1e12),
        kib=int(2**10),
        mib=int(2**20),
        gib=int(2**30),
        tib=int(2**40),
    )

    # Try casting size to value (will only work if no units)
    try:
        return int(float(size))
    except ValueError:
        pass

    # Ensure space between value and unit
    size = re.sub(r'([a-zA-Z]+)', r' \1', size.strip())

    parts = [s.strip() for s in size.split()]
    if len(parts) != 2:
        raise ValueError(
            'Input string "{size}" must contain only a value and a unit.',
        )

    value, unit = parts

    try:
        value_size = decimal.Decimal(value)
    except decimal.InvalidOperation as e:
        raise ValueError(f'Unable to interpret "{value}" as a float.') from e
    try:
        unit_size = units_to_bytes[unit.lower()]
    except KeyError as e:
        raise ValueError(f'Unknown unit type {unit}.') from e

    return int(value_size * unit_size)
