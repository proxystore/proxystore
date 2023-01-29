"""Endpoint constants."""
from __future__ import annotations

MAX_CHUNK_LENGTH = 16 * 1000 * 1000
"""Maximum chunk length (bytes) for GET/SET requests to/from the endpoint."""

MAX_OBJECT_SIZE_DEFAULT = int(1e9)
"""Default maximum endpoint object size in bytes."""
