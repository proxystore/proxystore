"""Endpoint to endpoint messages."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass
class EndpointRequest:
    """Message type for requests between endpoints.

    Attributes:
        kind: One of `#!python 'request'` or `#!python 'response'`.
        op: One of `#!python 'evict'`, `#!python 'exists'`, `#!python 'get'`,
            or `#!python 'set'`.
        uuid: UUID of sender.
        key: Key to operate on.
        data: Optional data to operate on.
        exists: Result of `exists` operation.
        error: Error raised by operation.
    """

    kind: Literal['request', 'response']
    op: Literal['evict', 'exists', 'get', 'set']
    uuid: str
    key: str
    data: bytes | None = None
    exists: bool | None = None
    error: Exception | None = None
