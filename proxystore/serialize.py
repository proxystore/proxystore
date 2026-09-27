"""Serialization functions."""

from __future__ import annotations

import dataclasses
import io
import pickle
import sys
from collections import OrderedDict
from collections.abc import Sized
from typing import Any
from typing import Protocol
from typing import runtime_checkable
from typing import TypeAlias

if sys.version_info >= (3, 12):  # pragma: >=3.12 cover
    from typing import TypeGuard
else:  # pragma: <3.12 cover
    from typing import TypeGuard

import cloudpickle

# Use at least pickle protocol 5 (added in Python 3.8), but prefer newer
# protocols if they become available.
_PICKLE_PROTOCOL = max(pickle.HIGHEST_PROTOCOL, 5)


if sys.version_info >= (3, 12):  # pragma: >=3.12 cover
    from collections.abc import Buffer

    @runtime_checkable
    class BytesLike(Buffer, Sized, Protocol):
        """Protocol for bytes-like objects."""

else:  # pragma: <3.12 cover
    BytesLike: TypeAlias = bytes | bytearray | memoryview
    """Protocol for bytes-like objects."""


class SerializationError(Exception):
    """Base Serialization Exception."""


@dataclasses.dataclass(frozen=True)
class _Data:
    """Serialized data of an object following the identifier line.

    Deserializing reads the data in place because copying the data of a
    large object is expensive.

    Attributes:
        buffer: Buffer passed to
            [`deserialize()`][proxystore.serialize.deserialize].
        start: Index of the first byte of data in `buffer`.
    """

    buffer: BytesLike
    start: int = 0

    def view(self) -> memoryview:
        """Get a view of the data without copying it."""
        return memoryview(self.buffer).cast('B')[self.start :]

    def file(self) -> io.BytesIO:
        """Get a file-like object of the data.

        [`io.BytesIO`][io.BytesIO] shares the memory of a [`bytes`][bytes]
        object, but copies any other type of buffer.
        """
        if isinstance(self.buffer, bytes):
            file = io.BytesIO(self.buffer)
            file.seek(self.start)
            return file
        return io.BytesIO(self.view())


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

    def deserialize(self, data: _Data) -> Any:
        """Deserialize data to an object."""
        ...


class _BytesSerializer:
    identifier = b'BS'
    name = 'bytes'

    def supported(self, obj: Any) -> bool:
        return isinstance(obj, bytes)

    def serialize(self, obj: Any, buffer: io.BytesIO) -> None:
        buffer.write(obj)

    def deserialize(self, data: _Data) -> Any:
        return bytes(data.view())


class _StrSerializer:
    identifier = b'US'
    name = 'string'

    def supported(self, obj: Any) -> bool:
        return isinstance(obj, str)

    def serialize(self, obj: Any, buffer: io.BytesIO) -> None:
        buffer.write(obj.encode())

    def deserialize(self, data: _Data) -> Any:
        return str(data.view(), 'utf-8')


# The numpy, pandas, and polars serializers import their library lazily.
# Importing these libraries is slow, and polars starts a thread pool on
# import which deadlocks processes that fork afterwards (e.g., the endpoint
# daemon). An object can only be an instance of a type from one of these
# libraries if the library has already been imported, so checking
# sys.modules in supported() never misses a compatible object.


class _NumpySerializer:
    identifier = b'NP'
    name = 'numpy'

    def supported(self, obj: Any) -> bool:
        np = sys.modules.get('numpy')
        return np is not None and isinstance(obj, np.ndarray)

    def serialize(self, obj: Any, buffer: io.BytesIO) -> None:
        import numpy as np

        # Must allow_pickle=True for the case where the numpy array contains
        # non-numeric data.
        np.save(buffer, obj, allow_pickle=True)

    def deserialize(self, data: _Data) -> Any:
        import numpy as np

        return np.load(data.file(), allow_pickle=True)


class _PandasSerializer:
    identifier = b'PD'
    name = 'pandas'

    def supported(self, obj: Any) -> bool:
        pd = sys.modules.get('pandas')
        return pd is not None and isinstance(obj, pd.DataFrame)

    def serialize(self, obj: Any, buffer: io.BytesIO) -> None:
        # Pandas with pickle protocol 5 is the suggested serialization
        # method for best efficiency. We tested feather IPC and parquet and
        # both were slower than pickle.
        # https://github.com/dask/distributed/issues/614#issuecomment-631033227
        obj.to_pickle(buffer, protocol=_PICKLE_PROTOCOL)

    def deserialize(self, data: _Data) -> Any:
        import pandas as pd

        return pd.read_pickle(data.file())


class _PolarsSerializer:
    identifier = b'PL'
    name = 'polars'

    def supported(self, obj: Any) -> bool:
        pl = sys.modules.get('polars')
        return pl is not None and isinstance(obj, pl.DataFrame)

    def serialize(self, obj: Any, buffer: io.BytesIO) -> None:
        obj.write_ipc(buffer)

    def deserialize(self, data: _Data) -> Any:
        import polars as pl

        return pl.read_ipc(bytes(data.view()))


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

    def deserialize(self, data: _Data) -> Any:
        return pickle.loads(data.view())


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

    def deserialize(self, data: _Data) -> Any:
        return cloudpickle.loads(data.view())


_SERIALIZERS: dict[bytes, _Serializer] = OrderedDict()
_MAX_IDENTIFIER_LINE = 16
# Maximum length of the identifier line (including the newline) read by
# deserialize(). Identifiers are two bytes by convention.


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
_register_serializer(_NumpySerializer)
_register_serializer(_PandasSerializer)
_register_serializer(_PolarsSerializer)
_register_serializer(_PickleSerializer)
_register_serializer(_CloudPickleSerializer)


def is_bytes_like(obj: Any) -> TypeGuard[BytesLike]:
    """Check if the object is bytes-like."""
    if sys.version_info >= (3, 12):  # pragma: >=3.12 cover
        return isinstance(obj, BytesLike)
    return isinstance(  # pragma: <3.12 cover
        obj,
        (bytes, bytearray, memoryview),
    )


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
        Bytes-like object that can be passed to \
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
                buffer = io.BytesIO()
                buffer.write(identifier + b'\n')
                serializer.serialize(obj, buffer)
                return buffer.getvalue()
            except Exception as e:  # noqa: BLE001
                last_exception = e

    assert last_exception is not None
    raise SerializationError(
        f'Object of type {type(obj)} is not supported.',
    ) from last_exception


def deserialize(buffer: BytesLike) -> Any:
    """Deserialize object.

    Warning:
        Pickled data is not secure, and malicious pickled object can execute
        arbitrary code when unpickled. Only unpickle data you trust.

    Args:
        buffer: Bytes-like object produced by
            [`serialize()`][proxystore.serialize.serialize].

    Returns:
        The deserialized object.

    Raises:
        ValueError: If `buffer` is not bytes-like.
        SerializationError: If the identifier of `buffer` is missing or
            invalid. The identifier is prepended to the string in
            [`serialize()`][proxystore.serialize.serialize] to indicate which
            serialization method was used (e.g., no serialization, pickle,
            etc.).
        SerializationError: If pickle or cloudpickle raise an exception
            when deserializing the object.
    """
    if not is_bytes_like(buffer):
        raise ValueError(
            f'Expected data to be a bytes-like type, not {type(buffer)}.',
        )

    # Only the identifier line is copied because copying the data of a
    # large object is expensive.
    head = bytes(memoryview(buffer).cast('B')[:_MAX_IDENTIFIER_LINE])
    end = head.find(b'\n')
    identifier = (head if end < 0 else head[:end]).strip()
    if end < 0 or identifier not in _SERIALIZERS:
        raise SerializationError(
            f'Unknown identifier {identifier!r} for deserialization.',
        )

    serializer = _SERIALIZERS[identifier]
    try:
        return serializer.deserialize(_Data(buffer, end + 1))
    except Exception as e:
        raise SerializationError(
            'Failed to deserialize object using the '
            f'{serializer.name} serializer.',
        ) from e
