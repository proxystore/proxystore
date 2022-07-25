"""CLI for serving an endpoint as a REST server."""
from __future__ import annotations

import json
import logging
import os
import uuid

import quart
from quart import request
from quart import Response

from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.exceptions import PeerRequestError
from proxystore.utils import chunk_bytes

logger = logging.getLogger(__name__)

routes_blueprint = quart.Blueprint('routes', __name__)

MAX_CHUNK_LENGTH = 16 * 1000 * 1000


def create_app(
    endpoint: Endpoint,
    max_content_length: int | None = None,
    body_timeout: int = 300,
) -> quart.Quart:
    """Creates quart app for endpoint and registers routes.

    Args:
        endpoint (Endpoint): initialized endpoint to forward quart routes to.
        max_content_length (int): max request body size in bytes
            (default: None).
        body_timeout (int): number of seconds to wait for the body to be
            completely received (default: 300)

    Returns:
        Quart app.
    """
    app = quart.Quart(__name__)

    app.config['endpoint'] = endpoint

    app.register_blueprint(routes_blueprint, url_prefix='')

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
    *,
    server: str | None = None,
    log_level: int | str = logging.INFO,
    log_file: str | None = None,
    max_memory: int | None = None,
    dump_dir: str | None = None,
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
        max_memory (int): optional max memory in bytes to use for storing
            objects. If exceeded, LRU objects will be dumped to `dump_dir`
            (default: None).
        dump_dir (str): optional directory to dump objects to if the
            memory limit is exceeded (default: None).
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

    endpoint = Endpoint(
        name=name,
        uuid=uuid,
        signaling_server=server,
        max_memory=max_memory,
        dump_dir=dump_dir,
    )
    app = create_app(endpoint)

    logger.info(
        f'serving endpoint {endpoint.uuid} ({endpoint.name}) on {host}:{port}',
    )
    app.run(host=host, port=port)


@routes_blueprint.before_app_serving
async def _startup() -> None:
    endpoint = quart.current_app.config['endpoint']
    await endpoint.async_init()


@routes_blueprint.after_app_serving
async def _shutdown() -> None:
    endpoint = quart.current_app.config['endpoint']
    await endpoint.close()


@routes_blueprint.route('/')
async def _home() -> tuple[str, int]:
    return ('', 200)


@routes_blueprint.route('/endpoint', methods=['GET'])
async def _endpoint_() -> Response:
    endpoint = quart.current_app.config['endpoint']
    return Response(
        json.dumps({'uuid': str(endpoint.uuid)}),
        200,
        content_type='application/json',
    )


@routes_blueprint.route('/evict', methods=['POST'])
async def _evict() -> Response:
    key = request.args.get('key', None)
    if key is None:
        return Response('request missing key', 400)

    endpoint_uuid: str | uuid.UUID | None = request.args.get(
        'endpoint',
        None,
    )
    endpoint = quart.current_app.config['endpoint']
    if isinstance(endpoint_uuid, str):
        try:
            endpoint_uuid = uuid.UUID(endpoint_uuid, version=4)
        except ValueError:
            return Response(f'{endpoint_uuid} is not a valid UUID4', 400)

    try:
        await endpoint.evict(key=key, endpoint=endpoint_uuid)
        return Response('', 200)
    except PeerRequestError as e:
        return Response(str(e), 400)


@routes_blueprint.route('/exists', methods=['GET'])
async def _exists() -> Response:
    key = request.args.get('key', None)
    if key is None:
        return Response('request missing key', 400)

    endpoint_uuid: str | uuid.UUID | None = request.args.get(
        'endpoint',
        None,
    )
    endpoint = quart.current_app.config['endpoint']
    if isinstance(endpoint_uuid, str):
        try:
            endpoint_uuid = uuid.UUID(endpoint_uuid, version=4)
        except ValueError:
            return Response(f'{endpoint_uuid} is not a valid UUID4', 400)

    try:
        exists = await endpoint.exists(key=key, endpoint=endpoint_uuid)
        return Response(
            json.dumps({'exists': exists}),
            200,
            content_type='application/json',
        )
    except PeerRequestError as e:
        return Response(str(e), 400)


@routes_blueprint.route('/get', methods=['GET'])
async def _get() -> Response:
    key = request.args.get('key', None)
    if key is None:
        return Response('request missing key', 400)

    endpoint_uuid: str | uuid.UUID | None = request.args.get(
        'endpoint',
        None,
    )
    endpoint = quart.current_app.config['endpoint']
    if isinstance(endpoint_uuid, str):
        try:
            endpoint_uuid = uuid.UUID(endpoint_uuid, version=4)
        except ValueError:
            return Response(f'{endpoint_uuid} is not a valid UUID4', 400)

    try:
        data = await endpoint.get(key=key, endpoint=endpoint_uuid)
    except PeerRequestError as e:
        return Response(str(e), 400)

    if data is not None:
        return Response(
            response=chunk_bytes(data, MAX_CHUNK_LENGTH),
            content_type='application/octet-stream',
        )
    else:
        return Response('', 400)


@routes_blueprint.route('/set', methods=['POST'])
async def _set() -> Response:
    key = request.args.get('key', None)
    if key is None:
        return Response('request missing key', 400)

    endpoint_uuid: str | uuid.UUID | None = request.args.get(
        'endpoint',
        None,
    )
    endpoint = quart.current_app.config['endpoint']
    if isinstance(endpoint_uuid, str):
        try:
            endpoint_uuid = uuid.UUID(endpoint_uuid, version=4)
        except ValueError:
            return Response(f'{endpoint_uuid} is not a valid UUID4', 400)

    data = bytearray()
    # Note: tests/endpoint/serve_test.py::test_empty_chunked_data handles
    # the branching case for where the code in the for loop is not executed
    # but coverage is not detecting that hence the pragma here
    async for chunk in request.body:  # pragma: no branch
        data += chunk

    if len(data) == 0:
        return Response('Received empty payload', 400)

    try:
        await endpoint.set(key=key, data=bytes(data), endpoint=endpoint_uuid)
    except PeerRequestError as e:
        return Response(str(e), 400)
    else:
        return Response('', 200)
