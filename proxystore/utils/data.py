"""Utilities for interacting with data."""
from __future__ import annotations

import decimal
import re
from typing import Generator


def chunk_bytes(
    data: bytes,
    chunk_size: int,
) -> Generator[bytes, None, None]:
    """Yield chunks of binary data.

    Args:
        data: Data to be chunked.
        chunk_size: Chunk size in bytes.

    Returns:
        Generator that yields chunks of bytes.
    """
    length = len(data)
    for index in range(0, length, chunk_size):
        yield data[index : min(index + chunk_size, length)]


def bytes_to_readable(size: int, precision: int = 3) -> str:
    """Convert bytes to human readable value.

    Note:
        This method uses base-10 values for KB, MB, GB, etc. instead of
        base-2 values (i.e., KiB, MiB, GiB, etc.).

    Args:
        size: Byte value to make readable.
        precision: Number of decimal places.

    Returns:
        String with human readable number of bytes.

    Raises:
        ValueError: If size is negative.
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
        ```python
        >>> readable_to_bytes('1.2 KB')
        1200
        >>> readable_to_bytes('0.6 MiB')
        629146
        ```

    Args:
        size: String to parse for bytes size.

    Returns:
        Integer number of bytes parsed from the string.

    Raises:
        ValueError: If the input string contains more than two parts (i.e., a
            value and a unit).
        ValueError: If the unit is not one of KB, MB, GB, TB, KiB, MiB, GiB,
            or TiB.
        ValueError: If the value cannot be cast to a float.
    """
    units_to_bytes = {
        'b': 1,
        'kb': int(1e3),
        'mb': int(1e6),
        'gb': int(1e9),
        'tb': int(1e12),
        'kib': int(2**10),
        'mib': int(2**20),
        'gib': int(2**30),
        'tib': int(2**40),
    }

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
