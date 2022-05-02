"""CLI for serving an endpoint as a REST server."""
from __future__ import annotations

import argparse
import logging
from typing import Sequence

import quart
from quart import request
from quart import Response

from proxystore.endpoint.endpoint import Endpoint


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

    @app.after_serving
    async def shutdown() -> None:
        await endpoint.close()

    @app.route('/')
    async def home() -> tuple[str, int]:
        return ('', 200)

    @app.route('/evict', methods=['POST'])
    async def evict() -> tuple[str, int]:
        await endpoint.evict(
            key=request.args.get('key'),
            endpoint=request.args.get('endpoint', None),
        )
        return ('', 200)

    @app.route('/exists', methods=['GET'])
    async def exists() -> tuple[dict[str, bool], int]:
        exists = await endpoint.exists(
            key=request.args.get('key'),
            endpoint=request.args.get('endpoint', None),
        )
        return ({'exists': exists}, 200)

    @app.route('/get', methods=['GET'])
    async def get() -> Response:
        data = await endpoint.get(
            key=request.args.get('key'),
            endpoint=request.args.get('endpoint', None),
        )
        return Response(
            response=data,
            content_type='application/octet-stream',
        )

    @app.route('/set', methods=['POST'])
    async def set() -> tuple[str, int]:
        await endpoint.set(
            key=request.args.get('key'),
            data=await request.get_data(),
            endpoint=request.args.get('endpoint', None),
        )
        return ('', 200)

    return app


def serve(host: str, port: int, signaling_server: str) -> None:
    """Initialize endpoint and serve Quart app.

    Args:
        host (str): host address to server Quart app on.
        port (int): port to serve Quart app on.
        signaling_server (str): address of signaling server that endpoint
            will register with and use for establishing peer to peer
            connections.
    """
    endpoint = Endpoint(signaling_server=signaling_server)
    app = create_app(endpoint)

    # TODO(gpauloski): handle sigterm/sigkill
    app.run(host=host, port=port)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI for starting an endpoint."""
    parser = argparse.ArgumentParser('ProxyStore Blobspace Endpoint')
    parser.add_argument(
        '--signaling-server',
        default=None,
        type=str,
        help=(
            'optional signaling server for p2p communication between endpoints'
        ),
    )
    parser.add_argument(
        '--host',
        default='0.0.0.0',
        help='host to listen on (defaults to 0.0.0.0 for all addresses)',
    )
    parser.add_argument(
        '--port',
        default=5000,
        type=int,
        help='port to listen on',
    )
    parser.add_argument(
        '--log-level',
        choices=['ERROR', 'WARNING', 'INFO', 'DEBUG'],
        default='INFO',
        help='logging level',
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=args.log_level)

    serve(
        host=args.host,
        port=args.port,
        signaling_server=args.signaling_server,
    )

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
