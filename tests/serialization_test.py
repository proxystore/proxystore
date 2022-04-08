"""Serialization Unit Tests."""
from __future__ import annotations

import numpy as np
from pytest import raises

from proxystore.serialize import deserialize
from proxystore.serialize import SerializationError
from proxystore.serialize import serialize


def test_serialization() -> None:
    """Test serialization."""
    xb = b'test string'
    b = serialize(xb)
    assert deserialize(b) == xb

    xs = 'test string'
    b = serialize(xs)
    assert deserialize(b) == xs

    xa = np.array([1, 2, 3])
    b = serialize(xa)
    assert np.array_equal(deserialize(b), xa)

    b = serialize(lambda: [1, 2, 3])
    f = deserialize(b)
    assert f() == [1, 2, 3]

    with raises(ValueError):
        # deserialize raises ValueError on non-bytes inputs
        deserialize('xxx')

    with raises(SerializationError):
        # No identifier
        deserialize(b'xxx')

    with raises(SerializationError):
        # Fake identifier 'xxx'
        deserialize(b'99\nxxx')
