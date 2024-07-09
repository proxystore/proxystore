"""Serialization functions."""

from __future__ import annotations

import pickle
from collections import OrderedDict
from typing import Any
from typing import Protocol

import cloudpickle


class SerializationError(Exception):
    """Base Serialization Exception."""

    pass


class _Serializer(Protocol):
    """Serializer protocol."""

    def supported(self, obj: Any) -> bool:
        """Check if the serializer is compatible with the object.

        The `supported` check is designed to fast way to determine if this
        serializer may be compatible with a given `obj`. If `supported(obj)`
        returns `False`, then it is guaranteed that `serialize(obj)` will
        fail. However, the contrapositive is not true. `serialize(obj)`
        can still fail even if `supported(obj)` return `True`.
        """
        ...

    def serialize(self, obj: Any) -> bytes:
        """Serialize the object to bytes."""
        ...

    def deserialize(self, data: bytes) -> Any:
        """Deserialize bytes to an object."""
        ...


class _BytesSerializer:
    def supported(self, obj: Any) -> bool:
        return isinstance(obj, bytes)

    def serialize(self, obj: Any) -> bytes:
        return obj

    def deserialize(self, data: bytes) -> Any:
        return data


class _StrSerializer:
    def supported(self, obj: Any) -> bool:
        return isinstance(obj, str)

    def serialize(self, obj: Any) -> bytes:
        return obj.encode()

    def deserialize(self, data: bytes) -> Any:
        return data.decode()


class _PickleSerializer:
    def supported(self, obj: Any) -> bool:
        # Assume this serializer can handle any type. This is not explicitly
        # true but checking every exception is non-trivial and essentially
        # requires attempting serialization and seeing if it fails.
        return True

    def serialize(self, obj: Any) -> bytes:
        return pickle.dumps(obj, protocol=5)

    def deserialize(self, data: bytes) -> Any:
        return pickle.loads(data)


class _CloudPickleSerializer:
    def supported(self, obj: Any) -> bool:
        # Assume this serializer can handle any type. This is not explicitly
        # true but checking every exception is non-trivial and essentially
        # requires attempting serialization and seeing if it fails.
        return True

    def serialize(self, obj: Any) -> bytes:
        return cloudpickle.dumps(obj)

    def deserialize(self, data: bytes) -> Any:
        return cloudpickle.loads(data)


_SERIALIZERS: dict[bytes, _Serializer] = OrderedDict(
    [
        (b'01', _BytesSerializer()),
        (b'02', _StrSerializer()),
        (b'03', _PickleSerializer()),
        (b'04', _CloudPickleSerializer()),
    ],
)


def serialize(obj: Any) -> bytes:
    """Serialize object.

    Objects are serialized with different mechanisms depending on their type.

      - [bytes][] types are not serialized.
      - [str][] types are encoded to bytes.
      - Other types are
        [pickled](https://docs.python.org/3/library/pickle.html){target=_blank}.
        If pickle fails,
        [cloudpickle](https://github.com/cloudpipe/cloudpickle){target=_blank}
        is used as a fallback.

    Args:
        obj: Object to serialize.

    Returns:
        Bytes that can be passed to \
        [`deserialize()`][proxystore.serialize.deserialize].

    Raises:
        SerializationError: If serializing the object fails with all available
            serializers. Cloudpickle is the last resort, so this error will
            typically be raised from a cloudpickle error.
    """
    last_exception: Exception | None = None
    for identifier, serializer in _SERIALIZERS.items():
        if serializer.supported(obj):
            try:
                data = serializer.serialize(obj)
                return identifier + b'\n' + data
            except Exception as e:
                last_exception = e

    assert last_exception is not None
    raise SerializationError(
        f'Object of type {type(obj)} is not supported.',
    ) from last_exception


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
        SerializationError: If pickle or cloudpickle raise an exception
            when deserializing the object.
    """
    if not isinstance(data, bytes):
        raise ValueError(
            f'Expected data to be of type bytes, not {type(data)}.',
        )
    identifier, _, data = data.partition(b'\n')
    if identifier not in _SERIALIZERS:
        raise SerializationError(
            f'Unknown identifier {identifier!r} for deserialization.',
        )
    serializer = _SERIALIZERS[identifier]
    try:
        return serializer.deserialize(data)
    except Exception as e:
        raise SerializationError(
            f'Failed to deserialize object with identifier {identifier!r}.',
        ) from e
