"""Serialization functions."""

from __future__ import annotations

import io
import pickle
from collections import OrderedDict
from typing import Any
from typing import Protocol

import cloudpickle

# Pickle protocol 5 is available in Python 3.8 version so that is ProxyStore's
# minimum version. If higher version come out in the future, prefer those.
_PICKLE_PROTOCOL = max(pickle.HIGHEST_PROTOCOL, 5)


class SerializationError(Exception):
    """Base Serialization Exception."""

    pass


class _Serializer(Protocol):
    """Serializer protocol.

    The `identifier` attribute, by convention, is a two-byte string containing
    a unique identifier for the serializer type. The name is the human-readable
    name of the serializer used in logging and error messages.
    """

    identifier: bytes
    name: str

    def supported(self, obj: Any) -> bool:
        """Check if the serializer is compatible with the object.

        The `supported` check is designed to be a fast way to determine if this
        serializer may be compatible with a given `obj`. If `supported(obj)`
        returns `False`, then it is guaranteed that `serialize(obj)` will
        fail. However, the contrapositive is not true. `serialize(obj)`
        can still fail even if `supported(obj)` return `True`.
        """
        ...

    def serialize(self, obj: Any, buffer: io.BytesIO) -> None:
        """Serialize the object and write to a buffer."""
        ...

    def deserialize(self, buffer: io.BytesIO) -> Any:
        """Deserialize bytes from a buffer to an object."""
        ...


class _BytesSerializer:
    identifier = b'BS'
    name = 'bytes'

    def supported(self, obj: Any) -> bool:
        return isinstance(obj, bytes)

    def serialize(self, obj: Any, buffer: io.BytesIO) -> None:
        buffer.write(obj)

    def deserialize(self, buffer: io.BytesIO) -> Any:
        return buffer.read()


class _StrSerializer:
    identifier = b'US'
    name = 'string'

    def supported(self, obj: Any) -> bool:
        return isinstance(obj, str)

    def serialize(self, obj: Any, buffer: io.BytesIO) -> None:
        buffer.write(obj.encode())

    def deserialize(self, buffer: io.BytesIO) -> Any:
        return buffer.read().decode()


class _NumpySerializer:
    identifier = b'NP'
    name = 'numpy'

    def supported(self, obj: Any) -> bool:
        return isinstance(obj, numpy.ndarray)

    def serialize(self, obj: Any, buffer: io.BytesIO) -> None:
        # Must allow_pickle=True for the case where the numpy array contains
        # non-numeric data.
        numpy.save(buffer, obj, allow_pickle=True)

    def deserialize(self, buffer: io.BytesIO) -> Any:
        return numpy.load(buffer, allow_pickle=True)


class _PandasSerializer:
    identifier = b'PD'
    name = 'pandas'

    def supported(self, obj: Any) -> bool:
        return isinstance(obj, pandas.DataFrame)

    def serialize(self, obj: Any, buffer: io.BytesIO) -> None:
        # Pandas with pickle protocol 5 is the suggested serialization
        # method for best efficiency. We tested feather IPC and parquet and
        # both were slower than pickle.
        # https://github.com/dask/distributed/issues/614#issuecomment-631033227
        obj.to_pickle(buffer, protocol=_PICKLE_PROTOCOL)

    def deserialize(self, buffer: io.BytesIO) -> Any:
        return pandas.read_pickle(buffer)


class _PolarsSerializer:
    identifier = b'PL'
    name = 'polars'

    def supported(self, obj: Any) -> bool:
        return isinstance(obj, polars.DataFrame)

    def serialize(self, obj: Any, buffer: io.BytesIO) -> None:
        obj.write_ipc(buffer)

    def deserialize(self, buffer: io.BytesIO) -> Any:
        return polars.read_ipc(buffer.read())


class _PickleSerializer:
    identifier = b'PK'
    name = 'pickle'

    def supported(self, obj: Any) -> bool:
        # Assume this serializer can handle any type. This is not explicitly
        # true but checking every exception is non-trivial and essentially
        # requires attempting serialization and seeing if it fails.
        return True

    def serialize(self, obj: Any, buffer: io.BytesIO) -> None:
        pickle.dump(obj, buffer, protocol=_PICKLE_PROTOCOL)

    def deserialize(self, buffer: io.BytesIO) -> Any:
        return pickle.load(buffer)


class _CloudPickleSerializer:
    identifier = b'CP'
    name = 'cloudpickle'

    def supported(self, obj: Any) -> bool:
        # Assume this serializer can handle any type. This is not explicitly
        # true but checking every exception is non-trivial and essentially
        # requires attempting serialization and seeing if it fails.
        return True

    def serialize(self, obj: Any, buffer: io.BytesIO) -> None:
        cloudpickle.dump(obj, buffer, protocol=_PICKLE_PROTOCOL)

    def deserialize(self, buffer: io.BytesIO) -> Any:
        return cloudpickle.load(buffer)


_SERIALIZERS: dict[bytes, _Serializer] = OrderedDict()


def _register_serializer(serializer: type[_Serializer]) -> None:
    if serializer.identifier in _SERIALIZERS:
        current = _SERIALIZERS[serializer.identifier]
        raise AssertionError(
            f'Serializer named {current.name!r} with identifier '
            f'{current.identifier!r} already exists.',
        )
    _SERIALIZERS[serializer.identifier] = serializer()


# Registration order determines priority so we register in the order
# we want serialization to be tried.
_register_serializer(_BytesSerializer)
_register_serializer(_StrSerializer)

try:
    import numpy

    _register_serializer(_NumpySerializer)
except ImportError:  # pragma: no cover
    pass

try:
    import pandas

    _register_serializer(_PandasSerializer)
except ImportError:  # pragma: no cover
    pass

try:
    import polars

    _register_serializer(_PolarsSerializer)
except ImportError:  # pragma: no cover
    pass

_register_serializer(_PickleSerializer)
_register_serializer(_CloudPickleSerializer)


def serialize(obj: Any) -> bytes:
    """Serialize object.

    Objects are serialized with different mechanisms depending on their type.

      - [bytes][] types are not serialized.
      - [str][] types are encoded to bytes.
      - [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html){target=_blank}
        types are serialized using
        [numpy.save](https://numpy.org/doc/stable/reference/generated/numpy.save.html){target=_blank}.
      - [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html){target=_blank}
        types are serialized using
        [to_pickle](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_pickle.html){target=_blank}.
      - [polars.DataFrame](https://pola-rs.github.io/polars/py-polars/html/reference/dataframe/index.html){target=_blank}
        types are serialized using
        [write_ipc](https://docs.pola.rs/api/python/stable/reference/api/polars.DataFrame.write_ipc.html){target=_blank}.
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
                with io.BytesIO() as buffer:
                    buffer.write(identifier + b'\n')
                    serializer.serialize(obj, buffer)
                    return buffer.getvalue()
            except Exception as e:
                last_exception = e

    assert last_exception is not None
    raise SerializationError(
        f'Object of type {type(obj)} is not supported.',
    ) from last_exception


def deserialize(data: bytes) -> Any:
    """Deserialize object.

    Warning:
        Pickled data is not secure, and malicious pickled object can execute
        arbitrary code when upickled. Only unpickle data you trust.

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

    with io.BytesIO(data) as buffer:
        identifier = buffer.readline().strip()
        if identifier not in _SERIALIZERS:
            raise SerializationError(
                f'Unknown identifier {identifier!r} for deserialization.',
            )

        serializer = _SERIALIZERS[identifier]
        try:
            return serializer.deserialize(buffer)
        except Exception as e:
            raise SerializationError(
                'Failed to deserialize object using the '
                f'{serializer.name} serializer.',
            ) from e
