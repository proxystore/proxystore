"""CLI for serving an endpoint as a REST server."""
from __future__ import annotations

import logging
import os
import uuid

import quart
from quart import request
from quart import Response

from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.exceptions import PeerRequestError

logger = logging.getLogger(__name__)

# Override Quart standard handlers
quart.logging.default_handler = logging.NullHandler()
quart.logging.serving_handler = logging.NullHandler()


def create_app(
    endpoint: Endpoint,
    max_content_length: int = 1000 * 1000 * 1000,
    body_timeout: int = 300,
) -> quart.Quart:
    """Creates quart app for endpoint and registers routes.

    Args:
        endpoint (Endpoint): initialized endpoint to forward quart routes to.
        max_content_length (int): max request body size in bytes
            (default: 1 GB).
        body_timeout (int): number of seconds to wait for the body to be
            completely received (default: 300)

    Returns:
        Quart app.
    """
    app = quart.Quart(__name__)

    # Propagate custom handlers to Quart App and Serving loggers
    app_logger = quart.logging.create_logger(app)
    serving_logger = quart.logging.create_serving_logger()
    app_logger.handlers = logger.handlers
    serving_logger.handlers = logger.handlers

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
        endpoint = request.args.get('endpoint', None)
        if endpoint is not None:
            try:
                endpoint = uuid.UUID(endpoint, version=4)
            except ValueError:
                return (f'{endpoint} is not a valid UUID4', 400)
        try:
            await app.endpoint.evict(
                key=request.args.get('key'),
                endpoint=endpoint,
            )
            return ('', 200)
        except PeerRequestError as e:
            return (str(e), 400)

    @app.route('/exists', methods=['GET'])
    async def exists() -> tuple[dict[str, bool] | str, int]:
        endpoint = request.args.get('endpoint', None)
        if endpoint is not None:
            try:
                endpoint = uuid.UUID(endpoint, version=4)
            except ValueError:
                return (f'{endpoint} is not a valid UUID4', 400)
        try:
            exists = await app.endpoint.exists(
                key=request.args.get('key'),
                endpoint=endpoint,
            )
            return ({'exists': exists}, 200)
        except PeerRequestError as e:
            return (str(e), 400)

    @app.route('/get', methods=['GET'])
    async def get() -> Response:
        endpoint = request.args.get('endpoint', None)
        if endpoint is not None:
            try:
                endpoint = uuid.UUID(endpoint, version=4)
            except ValueError:
                return (f'{endpoint} is not a valid UUID4', 400)
        try:
            data = await app.endpoint.get(
                key=request.args.get('key'),
                endpoint=endpoint,
            )
        except PeerRequestError as e:
            return (str(e), 400)
        if data is not None:
            return Response(
                response=data,
                content_type='application/octet-stream',
            )
        else:
            return ('', 400)

    @app.route('/set', methods=['POST'])
    async def set() -> tuple[str, int]:
        endpoint = request.args.get('endpoint', None)
        if endpoint is not None:
            try:
                endpoint = uuid.UUID(endpoint, version=4)
            except ValueError:
                return (f'{endpoint} is not a valid UUID4', 400)
        try:
            await app.endpoint.set(
                key=request.args.get('key'),
                data=await request.get_data(),
                endpoint=endpoint,
            )
            return ('', 200)
        except PeerRequestError as e:
            return (str(e), 400)

    logger.info(
        'quart routes registered to endpoint '
        f'{endpoint.uuid} ({endpoint.name})',
    )

    app.config['MAX_CONTENT_LENGTH'] = max_content_length
    app.config['BODY_TIMEOUT'] = body_timeout

    return app


def serve(
    name: str,
    uuid: uuid.UUID,
    host: str,
    port: int,
    server: str | None = None,
    log_level: int | str = logging.INFO,
    log_file: str | None = None,
) -> None:
    """Initialize endpoint and serve Quart app.

    Warning:
        This function does not return until the Quart app is terminated.

    Args:
        name (str): name of endpoint.
        uuid (str): uuid of endpoint.
        host (str): host address to server Quart app on.
        port (int): port to serve Quart app on.
        server (str): address of signaling server that endpoint
            will register with and use for establishing peer to peer
            connections. If None, endpoint will operate in solo mode (no
            peering) (default: None).
        log_level (int): logging level of endpoint (default: INFO).
        log_file (str): optional file path to append log to.
    """
    if log_file is not None:
        parent_dir = os.path.dirname(log_file)
        if not os.path.isdir(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)
        logging.getLogger().handlers.append(logging.FileHandler(log_file))

    for handler in logging.getLogger().handlers:
        handler.setFormatter(
            logging.Formatter(
                '[%(asctime)s.%(msecs)03d] %(levelname)-5s (%(name)s) :: '
                '%(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
            ),
        )
    logging.getLogger().setLevel(log_level)

    endpoint = Endpoint(name=name, uuid=uuid, signaling_server=server)
    app = create_app(endpoint)

    logger.info(
        f'serving endpoint {endpoint.uuid} ({endpoint.name}) on {host}:{port}',
    )
    app.run(host=host, port=port)
