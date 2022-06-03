"""CLI for serving an endpoint as a REST server."""
from __future__ import annotations

import logging

import quart
from quart import request
from quart import Response

from proxystore.endpoint.endpoint import Endpoint

logger = logging.getLogger(__name__)


def create_app(endpoint: Endpoint) -> quart.Quart:
    """Creates quart app for endpoint and registers routes.

    Args:
        endpoint (Endpoint): initialized endpoint to forward quart routes to.

    Returns:
        Quart app.
    """
    app = quart.Quart(__name__)

    @app.before_serving
    async def startup() -> None:
        await endpoint.async_init()
        app.endpoint = endpoint

    @app.after_serving
    async def shutdown() -> None:
        await app.endpoint.close()

    @app.route('/')
    async def home() -> tuple[str, int]:
        return ('', 200)

    @app.route('/endpoint', methods=['GET'])
    async def endpoint_() -> tuple[dict[str, str], int]:
        return ({'uuid': app.endpoint.uuid}, 200)

    @app.route('/evict', methods=['POST'])
    async def evict() -> tuple[str, int]:
        await app.endpoint.evict(
            key=request.args.get('key'),
            endpoint=request.args.get('endpoint', None),
        )
        return ('', 200)

    @app.route('/exists', methods=['GET'])
    async def exists() -> tuple[dict[str, bool], int]:
        exists = await app.endpoint.exists(
            key=request.args.get('key'),
            endpoint=request.args.get('endpoint', None),
        )
        return ({'exists': exists}, 200)

    @app.route('/get', methods=['GET'])
    async def get() -> Response:
        data = await app.endpoint.get(
            key=request.args.get('key'),
            endpoint=request.args.get('endpoint', None),
        )
        if data is not None:
            return Response(
                response=data,
                content_type='application/octet-stream',
            )
        else:
            return ('', 400)

    @app.route('/set', methods=['POST'])
    async def set() -> tuple[str, int]:
        await app.endpoint.set(
            key=request.args.get('key'),
            data=await request.get_data(),
            endpoint=request.args.get('endpoint', None),
        )
        return ('', 200)

    logger.info(
        'quart routes registered to endpoint '
        f'{endpoint.uuid} ({endpoint.name})',
    )

    return app


def serve(
    name: str,
    uuid: str,
    host: str,
    port: int,
    server: str | None = None,
) -> None:
    """Initialize endpoint and serve Quart app.

    Args:
        name (str): name of endpoint.
        uuid (str): uuid of endpoint.
        host (str): host address to server Quart app on.
        port (int): port to serve Quart app on.
        server (str): address of signaling server that endpoint
            will register with and use for establishing peer to peer
            connections. If None, endpoint will operate in solo mode (no
            peering) (default: None).
    """
    endpoint = Endpoint(name=name, uuid=uuid, signaling_server=server)
    app = create_app(endpoint)

    logger.info(
        f'serving endpoint {endpoint.uuid} ({endpoint.name}) on {host}:{port}',
    )
    app.run(host=host, port=port)
