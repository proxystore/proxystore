"""CLI for serving an endpoint as a REST server."""
from __future__ import annotations

import argparse
import logging
from typing import Sequence

import quart
from quart import request
from quart import Response

from proxystore.endpoint.config import update_config
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
    host: str,
    port: int,
    *,
    proxystore_dir: str | None = None,
    signaling_server: str | None = None,
) -> None:
    """Initialize endpoint and serve Quart app.

    Args:
        host (str): host address to server Quart app on.
        port (int): port to serve Quart app on.
        proxystore_dir (str): location to store proxystore endpoint data in.
            If not specified, defaults to :code:`$HOME/.proxystore`.
        signaling_server (str): address of signaling server that endpoint
            will register with and use for establishing peer to peer
            connections.
    """
    endpoint = Endpoint(
        endpoint_dir=proxystore_dir,
        signaling_server=signaling_server,
    )
    app = create_app(endpoint)

    # Update config so other processes can inspect filesystem to figure
    # out how to connect to this endpoint
    update_config(endpoint.endpoint_dir, host=host, port=port)

    # TODO(gpauloski): handle sigterm/sigkill
    logger.info(
        f'serving endpoint {endpoint.uuid} ({endpoint.name}) on {host}:{port}',
    )
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
        '--proxystore-dir',
        default=None,
        help='ProxyStore dir (defaults to $HOME/.proxystore.',
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
        proxystore_dir=args.proxystore_dir,
        signaling_server=args.signaling_server,
    )

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
