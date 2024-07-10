from __future__ import annotations

import io
from unittest import mock

import numpy
import pandas
import polars
import pytest

from proxystore.serialize import _NumpySerializer
from proxystore.serialize import _PandasSerializer
from proxystore.serialize import _PolarsSerializer
from proxystore.serialize import deserialize
from proxystore.serialize import SerializationError
from proxystore.serialize import serialize


def test_serialization() -> None:
    xb = b'test string'
    b = serialize(xb)
    assert deserialize(b) == xb

    xs = 'test string'
    b = serialize(xs)
    assert deserialize(b) == xs

    xa = [1, 2, 3]
    b = serialize(xa)
    assert deserialize(b) == xa

    b = serialize(lambda: [1, 2, 3])
    f = deserialize(b)
    assert f() == [1, 2, 3]

    xnp = numpy.array([1, 2, 3])
    b = serialize(xnp)
    assert numpy.array_equal(deserialize(b), xnp)

    xpd = pandas.DataFrame([[1, 2, 3], [4, 5, 6]])
    b = serialize(xpd)
    assert deserialize(b).equals(xpd)

    xpl = polars.DataFrame({'a': [1, 2, 3]})
    b = serialize(xpl)
    assert deserialize(b).equals(xpl)

    with pytest.raises(ValueError):
        # deserialize raises ValueError on non-bytes inputs
        deserialize('xxx')  # type: ignore

    with pytest.raises(SerializationError):
        # No identifier
        deserialize(b'xxx')

    with pytest.raises(SerializationError):
        # Fake identifier 'xxx'
        deserialize(b'99\nxxx')


def test_cloudpickle_dumps_error() -> None:
    with mock.patch('cloudpickle.dumps', side_effect=Exception()):
        with pytest.raises(
            SerializationError,
            match="Object of type <class 'function'> is not supported.",
        ):
            serialize(lambda x: x + x)  # pragma: no cover


def test_pickle_loads_error() -> None:
    v = serialize([1, 2, 3])
    with mock.patch('pickle.loads', side_effect=Exception()):
        with pytest.raises(
            SerializationError,
            match="Failed to deserialize object with identifier b'06'.",
        ):
            deserialize(v)


def test_cloudpickle_loads_error() -> None:
    v = serialize(lambda x: x + x)  # pragma: no cover
    with mock.patch('cloudpickle.loads', side_effect=Exception()):
        with pytest.raises(
            SerializationError,
            match="Failed to deserialize object with identifier b'07'.",
        ):
            deserialize(v)


def test_numpy_serializer() -> None:
    serializer = _NumpySerializer()
    xn = numpy.array([1, 2, 3])
    with io.BytesIO() as buffer:
        serializer.serialize(xn, buffer)
        buffer.seek(0)
        deserialized = serializer.deserialize(buffer)
        assert numpy.array_equal(xn, deserialized)


def test_pandas_serializer() -> None:
    serializer = _PandasSerializer()
    xp = pandas.DataFrame({'a': [1, 2, 3]})
    with io.BytesIO() as buffer:
        serializer.serialize(xp, buffer)
        buffer.seek(0)
        deserialized = serializer.deserialize(buffer)
        assert xp.equals(deserialized)


def test_polars_serializer() -> None:
    serializer = _PolarsSerializer()
    xpl = polars.DataFrame({'a': [1, 2, 3]})
    with io.BytesIO() as buffer:
        serializer.serialize(xpl, buffer)
        buffer.seek(0)
        deserialized = serializer.deserialize(buffer)
        assert xpl.equals(deserialized)


def test_numpy_supported() -> None:
    serializer = _NumpySerializer()
    assert serializer.supported(numpy.array([1, 2, 3]))
    assert not serializer.supported([1, 2, 3])


def test_pandas_supported() -> None:
    serializer = _PandasSerializer()
    assert serializer.supported(pandas.DataFrame({'a': [1, 2, 3]}))
    assert not serializer.supported({'a': [1, 2, 3]})


def test_polars_supported() -> None:
    serializer = _PolarsSerializer()
    assert serializer.supported(polars.DataFrame({'a': [1, 2, 3]}))
    assert not serializer.supported({'a': [1, 2, 3]})
