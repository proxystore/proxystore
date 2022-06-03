"""Message types for endpoint-to-endpoint operations."""
from __future__ import annotations

import sys
from dataclasses import dataclass

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    KW_ONLY_KWARG: dict[str, bool] = {'kw_only': True}
else:  # pragma: <3.10 cover
    KW_ONLY_KWARG: dict[str, bool] = {}


@dataclass(**KW_ONLY_KWARG)
class Request:
    """Base endpoint-to-endpoint request."""

    key: str
    _id: str | None = None


@dataclass(**KW_ONLY_KWARG)
class EvictRequest(Request):
    """Evict key from peer endpoint request."""

    pass


@dataclass(**KW_ONLY_KWARG)
class ExistsRequest(Request):
    """Check if key exists in peer endpoint request."""

    pass


@dataclass(**KW_ONLY_KWARG)
class GetRequest(Request):
    """Get value from peer endpoint request."""

    pass


@dataclass(**KW_ONLY_KWARG)
class SetRequest(Request):
    """Set value in peer endpoint request."""

    data: bytes | None = None


@dataclass(**KW_ONLY_KWARG)
class Response(Request):
    """Base response to request from peer endpoint."""

    pass


@dataclass(**KW_ONLY_KWARG)
class EvictResponse(Response):
    """Response to evict response from peer endpoint."""

    success: bool | None = None


@dataclass(**KW_ONLY_KWARG)
class ExistsResponse(Response):
    """Response to exists response from peer endpoint."""

    exists: bool | None = None


@dataclass(**KW_ONLY_KWARG)
class GetResponse(Response):
    """Response to get response from peer endpoint."""

    data: bytes | None = None


@dataclass(**KW_ONLY_KWARG)
class SetResponse(Response):
    """Response to set response from peer endpoint."""

    success: bool | None = None
