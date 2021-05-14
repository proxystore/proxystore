"""Serialization Utilities"""
import pickle

from typing import Any


class SerializationError(Exception):
    """Base Serialization Exception"""

    pass


def serialize(obj: Any) -> str:
    """Serialize object

    Args:
        obj: object to serialize.

    Returns:
        `str` that can be passed to `deserialize()`.
    """
    if isinstance(obj, bytes):
        identifier = '01\n'
        obj = obj.hex()
    elif isinstance(obj, str):
        identifier = '02\n'
    else:
        identifier = '03\n'
        obj = pickle.dumps(obj).hex()

    assert isinstance(obj, str)

    return identifier + obj


def deserialize(string: str) -> Any:
    """Deserialize object

    Args:
        string (str): string produced by `serialize()`.

    Returns:
        object that was serialized.

    Raises:
        ValueError:
            if `string` is not of type `str`.
        SerializationError:
            if the identifier of `string` is missing or invalid.
            The identifier is prepended to the string in `serialize()` to
            indicate which serialization method was used
            (e.g., no serialization, Pickle, etc.).
    """
    if not isinstance(string, str):
        raise ValueError(
            'deserialize only accepts str arguments, not '
            '{}'.format(type(string))
        )
    try:
        identifier, string = string.split('\n', 1)
    except ValueError:
        raise SerializationError(
            'String does not have required identifier for deserialization'
        )
    if identifier == '01':
        return bytes.fromhex(string)
    elif identifier == '02':
        return string
    elif identifier == '03':
        return pickle.loads(bytes.fromhex(string))
    else:
        raise SerializationError(
            'Unknown identifier {} for deserialization'.format(identifier)
        )
