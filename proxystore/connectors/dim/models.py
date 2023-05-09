"""Message types for communication with DIM servers."""
from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import NamedTuple

if sys.version_info >= (3, 9):  # pragma: >=3.9 cover
    from typing import Literal
else:  # pragma: <3.9 cover
    from typing_extensions import Literal


class DIMKey(NamedTuple):
    """Key to objects stored across `UCXConnector`s.

    Attributes:
        dim_type: Type of DIM this key belongs to.
        obj_id: Unique object key.
        size: Object size in bytes.
        peer_host: Hostname of peer where object is located.
        peer_port: Port of peer server where object is located.
    """

    dim_type: Literal['margo', 'ucx', 'zmq']
    obj_id: str
    size: int
    peer_host: str
    peer_port: int


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
