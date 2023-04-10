"""RPC and RPCResponse are the data structures used to communicate with a DIM server."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass
class RPC:
    """RPC is a request or response to/from a DIM server."""

    operation: Literal['exists', 'evict', 'get', 'set']
    key: str
    payload: bytes | None


@dataclass
class RPCResponse:
    """RPCResponse is a response from a DIM server."""

    operation: Literal['exists', 'evict', 'get', 'set']
    key: str
    result: bytes | None
    exception: Exception | None
    exists: bool | None
