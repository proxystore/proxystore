from __future__ import annotations

import io
from typing import Any
from unittest import mock

import numpy
import pandas
import polars
import pytest

from proxystore.serialize import _NumpySerializer
from proxystore.serialize import _PandasSerializer
from proxystore.serialize import _PolarsSerializer
from proxystore.serialize import _register_serializer
from proxystore.serialize import deserialize
from proxystore.serialize import SerializationError
from proxystore.serialize import serialize


def test_register_duplicate_identifiers() -> None:
    class _TestSerializer:
        identifier = _NumpySerializer.identifier
        name = 'test'

        def supported(self, obj: Any) -> bool:
            raise NotImplementedError

        def serialize(self, obj: Any, buffer: io.BytesIO) -> None:
            raise NotImplementedError

        def deserialize(self, buffer: io.BytesIO) -> Any:
            raise NotImplementedError

    error = "Serializer named 'numpy' with identifier b'NP' already exists."
    with pytest.raises(AssertionError, match=error):
        _register_serializer(_TestSerializer)


@pytest.mark.parametrize(
    'obj',
    (
        b'binary-string',
        'normal-string',
        [1, 2, 3],
        numpy.array([[1, 2, 3], [4, 5, 6]]),
        pandas.DataFrame([[1, 2, 3], [4, 5, 6]]),
        polars.DataFrame([[1, 2, 3], [4, 5, 6]]),
    ),
)
def test_serialize_objects(obj: Any) -> None:
    serialized = serialize(obj)
    deserialized = deserialize(serialized)

    if isinstance(obj, numpy.ndarray):
        assert numpy.array_equal(deserialized, obj)
    elif isinstance(obj, (pandas.DataFrame, polars.DataFrame)):
        assert deserialized.equals(obj)
    else:
        assert deserialized == obj


def test_serialize_lambda() -> None:
    b = serialize(lambda: [1, 2, 3])
    f = deserialize(b)
    assert f() == [1, 2, 3]


def test_deserialize_bad_input_type():
    with pytest.raises(ValueError):
        deserialize('non-bytes-input')  # type: ignore


def test_deserialize_bad_identifier():
    with pytest.raises(SerializationError):
        # No identifier
        deserialize(b'xxx')

    with pytest.raises(SerializationError):
        # Fake identifier
        deserialize(b'99\nxxx')


def test_propagate_cloudpickle_dumps_error() -> None:
    with mock.patch('cloudpickle.dump', side_effect=Exception()):
        with pytest.raises(
            SerializationError,
            match="Object of type <class 'function'> is not supported.",
        ):
            serialize(lambda x: x + x)  # pragma: no cover


def test_propagate_pickle_loads_error() -> None:
    v = serialize([1, 2, 3])
    with mock.patch('pickle.load', side_effect=Exception()):
        msg = 'Failed to deserialize object using the pickle serializer.'
        with pytest.raises(SerializationError, match=msg):
            deserialize(v)


def test_propagate_cloudpickle_loads_error() -> None:
    v = serialize(lambda x: x + x)  # pragma: no cover
    with mock.patch('cloudpickle.load', side_effect=Exception()):
        msg = 'Failed to deserialize object using the cloudpickle serializer.'
        with pytest.raises(SerializationError, match=msg):
            deserialize(v)


def test_numpy_supported() -> None:
    serializer = _NumpySerializer()
    assert serializer.supported(numpy.array([1, 2, 3]))
    assert not serializer.supported([1, 2, 3])


def test_numpy_serializer() -> None:
    serializer = _NumpySerializer()
    xn = numpy.array([1, 2, 3])
    with io.BytesIO() as buffer:
        serializer.serialize(xn, buffer)
        buffer.seek(0)
        deserialized = serializer.deserialize(buffer)
        assert numpy.array_equal(xn, deserialized)


def test_pandas_supported() -> None:
    serializer = _PandasSerializer()
    assert serializer.supported(pandas.DataFrame({'a': [1, 2, 3]}))
    assert not serializer.supported({'a': [1, 2, 3]})


def test_pandas_serializer() -> None:
    serializer = _PandasSerializer()
    xp = pandas.DataFrame({'a': [1, 2, 3]})
    with io.BytesIO() as buffer:
        serializer.serialize(xp, buffer)
        buffer.seek(0)
        deserialized = serializer.deserialize(buffer)
        assert xp.equals(deserialized)


def test_polars_supported() -> None:
    serializer = _PolarsSerializer()
    assert serializer.supported(polars.DataFrame({'a': [1, 2, 3]}))
    assert not serializer.supported({'a': [1, 2, 3]})


def test_polars_serializer() -> None:
    serializer = _PolarsSerializer()
    xpl = polars.DataFrame({'a': [1, 2, 3]})
    with io.BytesIO() as buffer:
        serializer.serialize(xpl, buffer)
        buffer.seek(0)
        deserialized = serializer.deserialize(buffer)
        assert xpl.equals(deserialized)
