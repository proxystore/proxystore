"""Serialization Utilities"""
import cloudpickle
import pickle

from typing import Any


class SerializationError(Exception):
    """Base Serialization Exception"""

    pass


def serialize(obj: Any) -> bytes:
    """Serialize object

    Args:
        obj: object to serialize.

    Returns:
        `bytes` that can be passed to `deserialize()`.
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
            obj = pickle.dumps(obj)
        except Exception:
            identifier = b'04\n'
            obj = cloudpickle.dumps(obj)

    assert isinstance(identifier, bytes)
    assert isinstance(obj, bytes)

    return identifier + obj


def deserialize(data: bytes) -> Any:
    """Deserialize object

    Args:
        data (bytes): bytes produced by `serialize()`.

    Returns:
        object that was serialized.

    Raises:
        ValueError:
            if `data` is not of type `bytes`.
        SerializationError:
            if the identifier of `data` is missing or invalid.
            The identifier is prepended to the string in `serialize()` to
            indicate which serialization method was used
            (e.g., no serialization, Pickle, etc.).
    """
    if not isinstance(data, bytes):
        raise ValueError(
            'deserialize only accepts bytes arguments, not '
            '{}'.format(type(data))
        )
    identifier, separator, data = data.partition(b'\n')
    if separator == b'' or len(identifier) != len(b'00'):
        raise SerializationError(
            'data does not have required identifier for deserialization'
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
            'Unknown identifier {} for deserialization'.format(identifier)
        )
