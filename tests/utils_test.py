"""Utils Unit Tests."""
from __future__ import annotations

import os
from unittest import mock

import pytest

from proxystore import utils
from proxystore.factory import SimpleFactory
from proxystore.utils import bytes_to_readable
from proxystore.utils import chunk_bytes
from proxystore.utils import home_dir
from proxystore.utils import readable_to_bytes


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


@pytest.mark.parametrize(
    'env,expected',
    (
        ({}, '~/.local/share/proxystore'),
        ({'PROXYSTORE_HOME': '/path/to/home'}, '/path/to/home'),
        ({'XDG_DATA_HOME': '/path/to/home'}, '/path/to/home/proxystore'),
        ({'PROXYSTORE_HOME': '/home1', 'XDG_DATA_HOME': '/home2'}, '/home1'),
    ),
)
def test_home_dir(env: dict[str, str], expected: str) -> None:
    # Unset $XDG_DATA_HOME and save for restoring later
    xdg_data_home = os.environ.pop('XDG_DATA_HOME', None)

    with mock.patch.dict(os.environ, env):
        path = home_dir()
        assert isinstance(path, str)
        assert os.path.isabs(path)
        assert path == os.path.expanduser(expected)

    # Exclude from coverage because not every OS will set $XDG_DATA_HOME
    if xdg_data_home is not None:  # pragma: no cover
        os.environ['XDG_DATA_HOME'] = xdg_data_home


@pytest.mark.parametrize(
    'value,precision,expected',
    (
        (0, 3, '0 B'),
        (1, 3, '1 B'),
        (int(1e3), 3, '1 KB'),
        (int(1e6), 3, '1 MB'),
        (int(1e9), 3, '1 GB'),
        (int(1e12), 3, '1 TB'),
        (int(1e15), 3, '1000 TB'),
        (1001, 3, '1.001 KB'),
        (1001, 1, '1 KB'),
        (1001, 0, '1 KB'),
        (123456789, 3, '123.457 MB'),
        (int(4e13), 3, '40 TB'),
    ),
)
def test_bytes_to_readable(value: int, precision: int, expected: str) -> None:
    assert bytes_to_readable(value, precision) == expected


def test_bytes_to_readable_exceptions() -> None:
    with pytest.raises(ValueError, match='negative'):
        bytes_to_readable(-1)


@pytest.mark.parametrize(
    'value,expected',
    (
        ('0 B', 0),
        ('1 B', 1),
        ('1 KB', int(1e3)),
        ('1 MB', int(1e6)),
        ('1 GB', int(1e9)),
        ('1 TB', int(1e12)),
        ('1KB', int(1e3)),
        ('1MB', int(1e6)),
        ('1GB', int(1e9)),
        ('1TB', int(1e12)),
        ('1000 TB', int(1e15)),
        ('1.001 KB', 1001),
        ('1.0001 KB', 1000),
        ('123.457 MB', 123457000),
        ('40 TB', 40000000000000),
        ('0', 0),
        ('1', 1),
        ('1000000000', 1000000000),
    ),
)
def test_readable_to_bytes(value: str, expected: int) -> None:
    assert readable_to_bytes(value) == expected


def test_readable_to_bytes_too_many_parts() -> None:
    with pytest.raises(ValueError, match='value and a unit'):
        readable_to_bytes('1 B GB')

    with pytest.raises(ValueError, match='value and a unit'):
        readable_to_bytes('GB')


def test_readable_to_bytes_unknown_unit() -> None:
    with pytest.raises(ValueError, match='Unknown unit'):
        readable_to_bytes('1 XB')


def test_readable_to_bytes_value_cast_failure() -> None:
    with pytest.raises(ValueError, match='float'):
        # Note that is letter o rather than zero
        readable_to_bytes('O B')
