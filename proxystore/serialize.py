"""Serialization functions."""
from __future__ import annotations

import pickle
from typing import Any

import cloudpickle


class SerializationError(Exception):
    """Base Serialization Exception."""

    pass


def serialize(obj: Any) -> bytes:
    """Serialize object.

    Objects are serialized using
    [pickle](https://docs.python.org/3/library/pickle.html){target=_blank}
    (protocol 4) except for [bytes][] or [str][] objects.
    If pickle fails,
    [cloudpickle](https://github.com/cloudpipe/cloudpickle){target=_blank}
    is used as a fallback.

    Args:
        obj: Object to serialize.

    Returns:
        Bytes that can be passed to \
        [`deserialize()`][proxystore.serialize.deserialize].
    """
    if isinstance(obj, bytes):
        identifier = b'01\n'
    elif isinstance(obj, str):
        identifier = b'02\n'
        obj = obj.encode()
    else:
        # Use cloudpickle if pickle fails
        try:
            identifier = b'03\n'
            # Pickle protocol 5 is available in Python 3.8 and later
            obj = pickle.dumps(obj, protocol=5)
        except Exception:
            identifier = b'04\n'
            obj = cloudpickle.dumps(obj)

    assert isinstance(identifier, bytes)
    assert isinstance(obj, bytes)

    return identifier + obj


def deserialize(data: bytes) -> Any:
    """Deserialize object.

    Args:
        data: Bytes produced by
            [`serialize()`][proxystore.serialize.serialize].

    Returns:
        The deserialized object.

    Raises:
        ValueError: If `data` is not of type `bytes`.
        SerializationError: If the identifier of `data` is missing or
            invalid. The identifier is prepended to the string in
            [`serialize()`][proxystore.serialize.serialize] to indicate which
            serialization method was used (e.g., no serialization, pickle,
            etc.).
    """
    if not isinstance(data, bytes):
        raise ValueError(
            f'Expected data to be of type bytes, not {type(data)}.',
        )
    identifier, separator, data = data.partition(b'\n')
    if separator == b'' or len(identifier) != len(b'00'):
        raise SerializationError(
            'Data does not have required identifier for deserialization.',
        )
    if identifier == b'01':
        return data
    elif identifier == b'02':
        return data.decode()
    elif identifier == b'03':
        return pickle.loads(data)
    elif identifier == b'04':
        return cloudpickle.loads(data)
    else:
        raise SerializationError(
            f'Unknown identifier {identifier!r} for deserialization,',
        )
