from __future__ import annotations

from unittest import mock

import pytest

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
            match="Object of type <class 'function'> is not serializable.",
        ):
            serialize(lambda x: x + x)  # pragma: no cover


def test_pickle_loads_error() -> None:
    v = serialize([1, 2, 3])
    with mock.patch('pickle.loads', side_effect=Exception()):
        with pytest.raises(
            SerializationError,
            match='Failed to deserialize object with pickle.',
        ):
            deserialize(v)


def test_cloudpickle_loads_error() -> None:
    v = serialize(lambda x: x + x)  # pragma: no cover
    with mock.patch('cloudpickle.loads', side_effect=Exception()):
        with pytest.raises(
            SerializationError,
            match='Failed to deserialize object with cloudpickle.',
        ):
            deserialize(v)
