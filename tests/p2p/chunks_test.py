from __future__ import annotations

import pytest

from proxystore.p2p.chunks import Chunk
from proxystore.p2p.chunks import ChunkDType
from proxystore.p2p.chunks import chunkify
from proxystore.p2p.chunks import reconstruct
from testing.compat import randbytes


@pytest.mark.parametrize('dtype', (bytes, str))
def test_chunk_to_bytes(dtype: bytes | str) -> None:
    data: bytes | str = randbytes(100) if dtype is bytes else 'abcdefghijkl'

    chunk = Chunk(1, 0, 1, data)

    if dtype is bytes:
        assert chunk.dtype is ChunkDType.BYTES
    elif dtype is str:
        assert chunk.dtype is ChunkDType.STRING
    else:
        raise AssertionError('Unreachable.')

    chunk_bytes = bytes(chunk)

    new_chunk = Chunk.from_bytes(chunk_bytes)

    assert chunk.stream_id == new_chunk.stream_id
    assert chunk.seq_id == new_chunk.seq_id
    assert chunk.seq_len == new_chunk.seq_len
    assert chunk.data == new_chunk.data
    assert chunk.dtype == new_chunk.dtype


def test_chunk_validation() -> None:
    with pytest.raises(ValueError):
        Chunk(0, 2, 1, '')


@pytest.mark.parametrize('dtype', (bytes, str))
def test_chunk_and_reconstruct(dtype: bytes | str) -> None:
    data: bytes | str = randbytes(1000) if dtype is bytes else 'x' * 1000

    chunks = list(chunkify(data, 100, 1))
    new_data = reconstruct(chunks)

    assert data == new_data


def test_reconstruct_validation() -> None:
    with pytest.raises(ValueError, match='empty'):
        reconstruct([])

    with pytest.raises(ValueError, match='expected'):
        reconstruct([Chunk(0, 0, 1, ''), Chunk(0, 0, 1, '')])
