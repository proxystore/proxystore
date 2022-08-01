"""Utils Unit Tests."""
from __future__ import annotations

import os

import pytest

from proxystore import utils
from proxystore.factory import SimpleFactory
from proxystore.utils import chunk_bytes


@pytest.mark.parametrize(
    'data_size,chunk_size',
    ((100, 1), (1000, 100), (1000, 128)),
)
def test_chunk_bytes(data_size: int, chunk_size: int) -> None:
    data = os.urandom(data_size)
    result = bytearray()
    for chunk in chunk_bytes(data, chunk_size):
        result += chunk
    assert data == result


def test_create_key() -> None:
    """Test create_key()."""
    assert isinstance(utils.create_key(42), str)


def test_fullname() -> None:
    """Test fullname()."""
    assert utils.fullname(SimpleFactory) == 'proxystore.factory.SimpleFactory'
    assert (
        utils.fullname(SimpleFactory('string'))
        == 'proxystore.factory.SimpleFactory'
    )
    assert utils.fullname('string') == 'str'
