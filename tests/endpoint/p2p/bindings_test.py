from __future__ import annotations

import contextlib
from collections.abc import Generator
from typing import Any
from unittest import mock

import pytest
from iroh import iroh_ffi

from proxystore.endpoint.p2p import bindings
from proxystore.endpoint.p2p.bindings import patch_buffer_write


class _PerByteBuilder:
    def __init__(self) -> None:
        self.data = bytearray()

    @contextlib.contextmanager
    def _reserve(self, num_bytes: int) -> Generator[None, None, None]:
        yield

    def write(self, value: bytes) -> None:
        with self._reserve(len(value)):
            for _, byte in enumerate(value):
                self.data.append(byte)


class _OtherBuilder(_PerByteBuilder):
    def write(self, value: bytes) -> None:
        self.data.extend(value)


@pytest.mark.parametrize(
    'data',
    (
        b'',
        b'abc',
        bytes(range(256)) * 100,
        bytearray(b'abc' * 10),
        memoryview(b'abc' * 10),
    ),
)
def test_bytes_round_trip(data: Any) -> None:
    # The bindings are patched when the manager is imported
    import proxystore.endpoint.p2p.manager  # noqa: F401

    converter = iroh_ffi._UniffiFfiConverterBytes
    assert converter.lift(converter.lower(data)) == bytes(data)


def test_patch_per_byte_write() -> None:
    builder: type[_PerByteBuilder] = type('Builder', (_PerByteBuilder,), {})
    assert patch_buffer_write(builder)
    assert builder.write is bindings._memmove_write
    # Already patched
    assert not patch_buffer_write(builder)


def test_skip_other_write() -> None:
    builder: type[_OtherBuilder] = type('Builder', (_OtherBuilder,), {})
    assert not patch_buffer_write(builder)
    assert builder.write is _OtherBuilder.write
    instance = builder()
    instance.write(b'abc')
    assert instance.data == b'abc'


def test_skip_missing_builder() -> None:
    with mock.patch.object(bindings, 'iroh_ffi', object()):
        assert not patch_buffer_write()
    assert not patch_buffer_write(object)


def test_skip_missing_source() -> None:
    builder: type[_PerByteBuilder] = type('Builder', (_PerByteBuilder,), {})
    with mock.patch('inspect.getsource', side_effect=OSError):
        assert not patch_buffer_write(builder)
    assert builder.write is _PerByteBuilder.write
    instance = builder()
    instance.write(b'abc')
    assert instance.data == b'abc'
