"""Message chunking utilities."""
from __future__ import annotations

import enum
import math
from struct import pack
from struct import unpack_from
from typing import Generator

CHUNK_HEADER_LENGTH = 2 + (4 * 4)
CHUNK_HEADER_FORMAT = '!HLLLL'


class ChunkDType(enum.Enum):
    """Data type contained in a Chunk."""

    BYTES = 1
    """Data is bytes."""
    STRING = 2
    """Data is a string."""


class Chunk:
    """Representation of a chunk of a message.

    Args:
        stream_id: Unique ID for the stream of chunks.
        seq_id: Sequence number for this chunk in the stream.
        seq_len: Length of the stream.
        data: Data for this chunk.
        dtype: Optionally specify data type otherwise inferred from data.

    Raises:
        ValueError: if the sequence ID is not less than the sequence length.
    """

    def __init__(
        self,
        stream_id: int,
        seq_id: int,
        seq_len: int,
        data: bytes | str,
        dtype: ChunkDType | None = None,
    ) -> None:
        if seq_len <= seq_id:
            raise ValueError(
                f'seq_id ({seq_id}) must be less than seq_len ({seq_len}).',
            )
        self.stream_id = stream_id
        self.seq_id = seq_id
        self.seq_len = seq_len
        self.data = data
        if dtype is None:
            self.dtype = (
                ChunkDType.BYTES
                if isinstance(data, bytes)
                else ChunkDType.STRING
            )
        else:
            self.dtype = dtype

    def __bytes__(self) -> bytes:
        """Pack the chunk into bytes."""
        length = CHUNK_HEADER_LENGTH + len(self.data)
        header = pack(
            CHUNK_HEADER_FORMAT,
            self.dtype.value,
            length,
            self.stream_id,
            self.seq_id,
            self.seq_len,
        )
        data = (
            self.data.encode('utf8')
            if isinstance(self.data, str)
            else self.data
        )
        chunk = header + data

        data += b'\x00' * (len(chunk) % 4)
        return chunk

    @classmethod
    def from_bytes(cls, chunk: bytes) -> Chunk:
        """Decode bytes into a Chunk."""
        (dtype_value, length, stream_id, seq_id, seq_len) = unpack_from(
            CHUNK_HEADER_FORMAT,
            chunk,
        )
        dtype = ChunkDType(dtype_value)
        chunk_data = chunk[CHUNK_HEADER_LENGTH:length]
        data: bytes | str
        if dtype is ChunkDType.STRING:
            data = chunk_data.decode('utf8')
        else:
            data = chunk_data
        return cls(
            stream_id=stream_id,
            seq_id=seq_id,
            seq_len=seq_len,
            data=data,
            dtype=dtype,
        )


def chunkify(
    data: bytes | str,
    size: int,
    stream_id: int,
) -> Generator[Chunk, None, None]:
    """Generate chunks from data.

    Args:
        data: Data to chunk.
        size: Size of each chunk.
        stream_id: Unique ID for the stream of chunks.

    Yields:
        Chunks of data.
    """
    seq_len = math.ceil(len(data) / size)

    for i, x in enumerate(range(0, len(data), size)):
        chunk_data = data[x : min(x + size, len(data))]
        yield Chunk(
            stream_id=stream_id,
            seq_id=i,
            seq_len=seq_len,
            data=chunk_data,
        )


def reconstruct(chunks: list[Chunk]) -> bytes | str:
    """Reconstructs data from list of chunks.

    Args:
        chunks: List of chunks to order and join.

    Returns:
        Reconstructed bytes or string.
    """
    if len(chunks) == 0:
        raise ValueError('Chunks list cannot be empty.')
    seq_len = chunks[0].seq_len
    if len(chunks) != seq_len:
        raise ValueError(f'Got {len(chunks)} but expected {seq_len}.')
    chunks = sorted(chunks, key=lambda c: c.seq_id)
    if isinstance(chunks[0].data, bytes):
        return b''.join(c.data for c in chunks)  # type: ignore
    elif isinstance(chunks[0].data, str):
        return ''.join(c.data for c in chunks)  # type: ignore
    else:
        raise AssertionError('Unreachable.')
