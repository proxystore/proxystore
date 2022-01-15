"""Serialization Unit Tests."""
import numpy as np
from pytest import raises

from proxystore.serialize import deserialize
from proxystore.serialize import SerializationError
from proxystore.serialize import serialize


def test_serialization() -> None:
    """Test serialization."""
    x = b"test string"
    b = serialize(x)
    assert deserialize(b) == x

    x = "test string"
    b = serialize(x)
    assert deserialize(b) == x

    x = np.array([1, 2, 3])
    b = serialize(x)
    assert np.array_equal(deserialize(b), x)

    b = serialize(lambda: [1, 2, 3])
    f = deserialize(b)
    assert f() == [1, 2, 3]

    with raises(ValueError):
        # deserialize raises ValueError on non-bytes inputs
        deserialize("xxx")

    with raises(SerializationError):
        # No identifier
        deserialize(b"xxx")

    with raises(SerializationError):
        # Fake identifer 'xxx'
        deserialize(b"99\nxxx")
