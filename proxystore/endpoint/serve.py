"""Endpoint serving."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from typing import Any
from typing import Literal

try:
    import quart
    import uvicorn
    import uvloop
    from quart import request
    from quart import Response
except ImportError as e:  # pragma: no cover
    # Usually we would just print a warning, but this file requires
    # quart to be available to register functions to a top-level blueprint.
    raise ImportError(
        f'{e}. To enable endpoint serving, install proxystore with '
        '"pip install proxystore[endpoints]".',
    ) from e

from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.constants import MAX_CHUNK_LENGTH
from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.exceptions import PeerRequestError
from proxystore.endpoint.storage import DictStorage
from proxystore.endpoint.storage import SQLiteStorage
from proxystore.endpoint.storage import Storage
from proxystore.globus.client import is_client_login
from proxystore.globus.manager import ConfidentialAppAuthManager
from proxystore.globus.manager import GlobusAuthManager
from proxystore.globus.manager import NativeAppAuthManager
from proxystore.globus.scopes import ProxyStoreRelayScopes
from proxystore.p2p.manager import PeerManager
from proxystore.p2p.nat import check_nat_and_log
from proxystore.p2p.relay.client import RelayClient
from proxystore.utils.data import chunk_bytes

logger = logging.getLogger(__name__)

routes_blueprint = quart.Blueprint('routes', __name__)


def create_app(
    endpoint: Endpoint,
    max_content_length: int | None = None,
    body_timeout: int = 300,
) -> quart.Quart:
    """Create quart app for endpoint and registers routes.

    Args:
        endpoint: Initialized endpoint to forward quart routes to.
        max_content_length: Max request body size in bytes.
        body_timeout: Number of seconds to wait for the body to be
            completely received.

    Returns:
        Quart app.
    """
    app = quart.Quart(__name__)

    app.config['endpoint'] = endpoint

    app.register_blueprint(routes_blueprint, url_prefix='')

    app.config['MAX_CONTENT_LENGTH'] = max_content_length
    app.config['BODY_TIMEOUT'] = body_timeout

    return app


def _get_auth_headers(
    method: Literal['globus'] | None,
    **kwargs: Any,
) -> dict[str, str]:
    if method is None:
        return {}
    elif method == 'globus':
        manager: GlobusAuthManager
        if is_client_login():
            logger.info('Using confidential app Globus Auth client')
            manager = ConfidentialAppAuthManager()
        else:
            logger.info('Using native app Globus Auth client')
            manager = NativeAppAuthManager()
        resource_server = kwargs.get(
            'resource_server',
            ProxyStoreRelayScopes.resource_server,
        )
        try:
            authorizer = manager.get_authorizer(resource_server)
        except LookupError as e:
            logger.error(
                'Failed to find Globus Auth tokens for the specified relay '
                'resource server. Have you logged in yet? If not, login then '
                'try again.\n  $ proxystore-globus-auth login',
            )
            raise SystemExit(1) from e
        bearer = authorizer.get_authorization_header()
        assert bearer is not None
        return {'Authorization': bearer}
    else:
        raise AssertionError('Unreachable.')


async def _serve_async(config: EndpointConfig) -> None:
    if config.host is None:
        raise ValueError('EndpointConfig has NoneType as host.')

    storage: Storage | None
    database_path = config.storage.database_path
    if database_path is not None:
        logger.info(
            f'Using SQLite database for storage (path: {database_path})',
        )
        storage = SQLiteStorage(
            database_path,
            max_object_size=config.storage.max_object_size,
        )
    else:
        logger.warning(
            'Database path not provided. Data will not be persisted',
        )
        storage = DictStorage(max_object_size=config.storage.max_object_size)

    peer_manager: PeerManager | None = None
    if config.relay.address is not None:
        headers = _get_auth_headers(
            method=config.relay.auth.method,
            **config.relay.auth.kwargs,
        )
        relay_client = RelayClient(
            address=config.relay.address,
            client_name=config.name,
            client_uuid=uuid.UUID(config.uuid),
            extra_headers=headers,
            verify_certificate=config.relay.verify_certificate,
        )
        peer_manager = PeerManager(
            relay_client,
            peer_channels=config.relay.peer_channels,
        )
        check_nat_and_log()

    endpoint = await Endpoint(
        name=config.name,
        uuid=uuid.UUID(config.uuid),
        peer_manager=peer_manager,
        storage=storage,
    )
    app = create_app(endpoint)

    server_config = uvicorn.Config(
        app,
        host=config.host,
        port=config.port,
        log_config=None,
        log_level=logger.level,
        access_log=False,
    )
    server = uvicorn.Server(server_config)

    logger.info(
        f'Serving endpoint {uuid.UUID(config.uuid)} ({config.name}) on '
        f'{config.host}:{config.port}',
    )
    logger.info(f'Config: {config}')

    await server.serve()


def serve(
    config: EndpointConfig,
    *,
    log_level: int | str = logging.INFO,
    log_file: str | None = None,
    use_uvloop: bool = True,
) -> None:
    """Initialize endpoint and serve Quart app.

    Warning:
        This function does not return until the Quart app is terminated.

    Args:
        config: Configuration object.
        log_level: Logging level of endpoint.
        log_file: Optional file path to append log to.
        use_uvloop: Install uvloop as the default event loop implementation.
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

    if use_uvloop:  # pragma: no cover
        logger.info('Installing uvloop as default event loop')
        uvloop.install()
    else:
        logger.warning(
            'Not installing uvloop. Uvicorn may override and install anyways',
        )

    # The remaining set up and serving code is deferred to within the
    # _serve_async helper function which will be executed within an event loop.
    try:
        asyncio.run(_serve_async(config))
    except Exception as e:
        # Intercept exception so we can log it in the case that the endpoint
        # is running as a daemon process. Otherwise the user will never see
        # the exception.
        logger.exception(f'Caught unhandled exception: {e!r}')
        raise
    finally:
        logger.info(f'Finished serving endpoint: {config.name}')


@routes_blueprint.before_app_serving
async def _startup() -> None:
    endpoint = quart.current_app.config['endpoint']
    # Typically async_init() is called when the endpoint is initialized
    # with the await keyword, but we call it again here in case the endpoint
    # object needed to be initialized outside of an event loop.
    await endpoint.async_init()


@routes_blueprint.after_app_serving
async def _shutdown() -> None:
    endpoint = quart.current_app.config['endpoint']
    await endpoint.close()


@routes_blueprint.route('/')
async def _home() -> tuple[str, int]:
    return ('', 200)


@routes_blueprint.route('/endpoint', methods=['GET'])
async def endpoint_handler() -> Response:
    """Route handler for `GET /endpoint`.

    Responses:

    * `Status Code 200`: JSON containing the key `uuid` with the value as
      the string UUID of this endpoint.
    """
    endpoint = quart.current_app.config['endpoint']
    return Response(
        json.dumps({'uuid': str(endpoint.uuid)}),
        200,
        content_type='application/json',
    )


@routes_blueprint.route('/evict', methods=['POST'])
async def evict_handler() -> Response:
    """Route handler for `POST /evict`.

    Responses:

    * `Status Code 200`: If the operation succeeds. The response message will
      be empty.
    * `Status Code 400`: If the key argument is missing or the endpoint UUID
      argument is present but not a valid UUID.
    * `Status Code 500`: If there was a peer request error. The response
      will contain the string representation of the internal error.
    """
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
        return Response(str(e), 500)


@routes_blueprint.route('/exists', methods=['GET'])
async def exists_handler() -> Response:
    """Route handler for `GET /exists`.

    Responses:

    * `Status Code 200`: If the operation succeeds. The response message will
      be empty.
    * `Status Code 400`: If the key argument is missing or the endpoint UUID
      argument is present but not a valid UUID.
    * `Status Code 500`: If there was a peer request error. The response
      will contain the string representation of the internal error.
    """
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
        return Response(str(e), 500)


@routes_blueprint.route('/get', methods=['GET'])
async def get_handler() -> Response:
    """Route handler for `GET /get`.

    Responses:

    * `Status Code 200`: If the operation succeeds. The response message will
       contain the octet-stream of the requested data.
    * `Status Code 400`: If the key argument is missing or the endpoint UUID
      argument is present but not a valid UUID.
    * `Status Code 404`: If there is no data associated with the provided key.
    * `Status Code 500`: If there was a peer request error. The response
      will contain the string representation of the internal error.
    """
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
        return Response(str(e), 500)

    if data is not None:
        return Response(
            response=chunk_bytes(data, MAX_CHUNK_LENGTH),
            content_type='application/octet-stream',
        )
    else:
        return Response('no data associated with request key', 404)


@routes_blueprint.route('/set', methods=['POST'])
async def set_handler() -> Response:
    """Route handler for `POST /set`.

    Responses:

    * `Status Code 200`: If the operation succeeds. The response message will
      be empty.
    * `Status Code 400`: If the key argument is missing, the endpoint UUID
      argument is present but not a valid UUID, or the request is missing
      the data payload.
    * `Status Code 500`: If there was a peer request error. The response
      will contain the string representation of the internal error.
    """
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
        return Response('received empty payload', 400)

    try:
        await endpoint.set(key=key, data=bytes(data), endpoint=endpoint_uuid)
    except PeerRequestError as e:
        return Response(str(e), 500)
    else:
        return Response('', 200)
