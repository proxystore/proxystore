"""Message types for communication with DIM servers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from typing import NamedTuple


class DIMKey(NamedTuple):
    """Key to objects stored across `UCXConnector`s."""

    dim_type: Literal['margo', 'ucx', 'zmq']
    """Type of DIM this key belongs to."""
    obj_id: str
    """Unique object key."""
    size: int
    """Object size in bytes."""
    peer_host: str
    """Hostname of peer where object is located."""
    peer_port: int
    """Port of peer server where object is located."""


@dataclass
class RPC:
    """Client request to a DIM server.

    Attributes:
        operation: Operation type requested.
        key: Key to operate on.
        size: Size of data associated with key.
        data: Data associated with `set` operation.
    """

    operation: Literal['exists', 'evict', 'get', 'put']
    key: DIMKey
    data: bytes | None = None


@dataclass
class RPCResponse:
    """Server response to a client request.

    Attributes:
        operation: Operation type performed.
        key: Key that was operated on.
        size: Size of data associated with key.
        data: Data returned by `get` operation.
        exists: Return value for `exists` operation.
        exception: Optional exception raised by the operation.
    """

    operation: Literal['exists', 'evict', 'get', 'put']
    key: DIMKey
    data: bytes | None = None
    exists: bool | None = None
    exception: Exception | None = None
