"""Endpoint to endpoint messages."""
from __future__ import annotations

import sys
from dataclasses import dataclass

if sys.version_info >= (3, 8):  # pragma: >=3.8 cover
    from typing import Literal
else:  # pragma: <3.8 cover
    from typing_extensions import Literal


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
