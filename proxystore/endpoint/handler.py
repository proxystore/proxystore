"""Handle requests to an endpoint.

Warning:
    This module is an internal implementation detail. Its interface may
    change between releases without notice (see
    [`proxystore.endpoint`][proxystore.endpoint]).

Requests from clients (see
[`ClientHandler`][proxystore.endpoint.server.ClientHandler]) and from peer
endpoints (see [`PeerManager`][proxystore.p2p.manager.PeerManager]) are
both handled by
[`handle_request()`][proxystore.endpoint.handler.handle_request] which
dispatches the request to the typed methods of the
[`Endpoint`][proxystore.endpoint.endpoint.Endpoint] and converts the result,
or error, into a response.

To add an operation, add an [`Op`][proxystore.endpoint.protocol.Op], a
method to the [`Endpoint`][proxystore.endpoint.endpoint.Endpoint] and
[`EndpointClient`][proxystore.endpoint.client.EndpointClient], and a handler
in this module.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable
from collections.abc import Callable
from typing import TYPE_CHECKING

from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.exceptions import ObjectSizeExceededError
from proxystore.endpoint.exceptions import PeerError
from proxystore.endpoint.protocol import Message
from proxystore.endpoint.protocol import Op
from proxystore.endpoint.protocol import Request
from proxystore.endpoint.protocol import Status

if TYPE_CHECKING:
    from proxystore.endpoint.endpoint import Endpoint

logger = logging.getLogger(__name__)

_Data = bytes | bytearray

_Handler = Callable[['Endpoint', Request, _Data], Awaitable[Message]]


def _key(request: Request) -> str:
    if request.key is None:
        raise EndpointProtocolError('Request requires a key.')
    return request.key


async def _get(endpoint: Endpoint, request: Request, _: _Data) -> Message:
    result = await endpoint.get(_key(request), target=request.target)
    if result is None:
        return Message(Status.NOT_FOUND)
    return Message(Status.OK, data=result)


async def _set(endpoint: Endpoint, request: Request, data: _Data) -> Message:
    await endpoint.set(_key(request), data, target=request.target)
    return Message(Status.OK)


async def _exists(
    endpoint: Endpoint,
    request: Request,
    _: _Data,
) -> Message:
    exists = await endpoint.exists(_key(request), target=request.target)
    return Message(Status.OK, {'exists': exists})


async def _evict(
    endpoint: Endpoint,
    request: Request,
    _: _Data,
) -> Message:
    await endpoint.evict(_key(request), target=request.target)
    return Message(Status.OK)


async def _ping(endpoint: Endpoint, request: Request, _: _Data) -> Message:
    result = await endpoint.ping(request.target)
    return Message(Status.OK, result.to_meta())


_HANDLERS: dict[int, _Handler] = {
    Op.GET: _get,
    Op.SET: _set,
    Op.EXISTS: _exists,
    Op.EVICT: _evict,
    Op.PING: _ping,
}


async def handle_request(
    endpoint: Endpoint,
    request: Message,
    *,
    forward: bool,
) -> Message:
    """Handle a request to an endpoint.

    Args:
        endpoint: Endpoint to perform the request on.
        request: Request message.
        forward: Allow the request to be forwarded to a peer endpoint.
            Requests from clients can be forwarded but requests from peers
            are never forwarded again.

    Returns:
        The response message. Errors are returned as responses rather than
        raised.
    """
    handler = _HANDLERS.get(request.code)
    if handler is None:
        return Message.error(
            Status.BAD_REQUEST,
            f'unknown op {request.code}',
        )

    try:
        meta = Request.from_meta(request.meta)
        if (
            not forward
            and meta.target is not None
            and meta.target != endpoint.id
        ):
            raise EndpointProtocolError(
                'requests from peers cannot be forwarded',
            )
        return await handler(endpoint, meta, request.data)
    except EndpointProtocolError as e:
        return Message.error(Status.BAD_REQUEST, str(e))
    except ObjectSizeExceededError as e:
        return Message.error(Status.TOO_LARGE, str(e))
    except PeerError as e:
        return Message.error(Status.ERROR, str(e))
    except Exception as e:
        logger.exception(
            'Unexpected error handling op %s request', request.code
        )
        return Message.error(Status.ERROR, f'unexpected error: {e!r}')
