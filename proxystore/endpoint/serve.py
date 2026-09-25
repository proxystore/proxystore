"""Endpoint serving.

Endpoints serve client requests over TCP using the protocol defined in
[`proxystore.endpoint.protocol`][proxystore.endpoint.protocol].
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import socket
import uuid
from typing import Any
from typing import Literal

try:
    import uvloop
except ImportError as e:  # pragma: no cover
    raise ImportError(
        f'{e}. To enable endpoint serving, install proxystore with '
        '"pip install proxystore[endpoints]".',
    ) from e

from aiortc import RTCIceServer
from globus_sdk.token_storage import TokenValidationError

from proxystore.endpoint.auth import compute_proof
from proxystore.endpoint.auth import generate_token_file
from proxystore.endpoint.auth import verify_proof
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import get_token_filepath
from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.exceptions import ObjectSizeExceededError
from proxystore.endpoint.exceptions import PeerRequestError
from proxystore.endpoint.protocol import decode_meta
from proxystore.endpoint.protocol import HEADER
from proxystore.endpoint.protocol import Header
from proxystore.endpoint.protocol import local_versions
from proxystore.endpoint.protocol import NONCE_SIZE
from proxystore.endpoint.protocol import Op
from proxystore.endpoint.protocol import pack_message
from proxystore.endpoint.protocol import pack_preamble
from proxystore.endpoint.protocol import PREAMBLE
from proxystore.endpoint.protocol import PROTOCOL_VERSION
from proxystore.endpoint.protocol import Status
from proxystore.endpoint.protocol import unpack_header
from proxystore.endpoint.protocol import unpack_preamble
from proxystore.endpoint.storage import DictStorage
from proxystore.endpoint.storage import SQLiteStorage
from proxystore.endpoint.storage import Storage
from proxystore.globus.app import get_globus_app
from proxystore.globus.scopes import get_relay_scopes_by_resource_server
from proxystore.p2p.manager import PeerManager
from proxystore.p2p.nat import check_nat_and_log
from proxystore.p2p.relay.client import RelayClient

logger = logging.getLogger(__name__)

HANDSHAKE_TIMEOUT = 10
"""Seconds a client has to complete the handshake after connecting."""

_Response = tuple[Status, dict[str, Any] | None, bytes | None]


class EndpointServer:
    """Handles client connections to an endpoint.

    Pass the
    [`handle_connection()`][proxystore.endpoint.serve.EndpointServer.handle_connection]
    method as the callback to [`asyncio.start_server()`][asyncio.start_server].

    Args:
        endpoint: Endpoint to forward client requests to.
        token: Token that clients must prove they know.
        max_object_size: Optional maximum size in bytes of objects that
            clients can set. Requests exceeding this size are rejected
            before the data is read.
        handshake_timeout: Seconds a client has to complete the handshake.
    """

    def __init__(
        self,
        endpoint: Endpoint,
        token: bytes,
        *,
        max_object_size: int | None = None,
        handshake_timeout: float = HANDSHAKE_TIMEOUT,
    ) -> None:
        self.endpoint = endpoint
        self.token = token
        self.max_object_size = max_object_size
        self.handshake_timeout = handshake_timeout
        self._connections: set[asyncio.StreamWriter] = set()

    def close_connections(self) -> None:
        """Close all open client connections."""
        for writer in list(self._connections):
            writer.close()

    async def handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle a client connection until it is closed."""
        self._connections.add(writer)
        peer = writer.get_extra_info('peername')
        sock = writer.get_extra_info('socket')
        if sock is not None:  # pragma: no branch
            with contextlib.suppress(OSError):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        try:
            try:
                authenticated = await asyncio.wait_for(
                    self._handshake(reader, writer, peer),
                    timeout=self.handshake_timeout,
                )
            except TimeoutError:
                logger.warning(
                    f'Closing connection from {peer} because the client did '
                    f'not complete the handshake within '
                    f'{self.handshake_timeout} seconds',
                )
                return
            if authenticated:
                await self._serve_requests(reader, writer)
        except (
            ConnectionError,
            asyncio.IncompleteReadError,
            EndpointProtocolError,
        ) as e:
            logger.debug(f'Closing connection from {peer}: {e!r}')
        finally:
            self._connections.discard(writer)
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _handshake(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        peer: Any,
    ) -> bool:
        version = unpack_preamble(await reader.readexactly(PREAMBLE.size))
        if version != PROTOCOL_VERSION:
            logger.warning(
                f'Rejecting connection from {peer} with protocol version '
                f'{version} (expected {PROTOCOL_VERSION})',
            )
            error = (
                f'Endpoint uses protocol version {PROTOCOL_VERSION} but the '
                f'client uses protocol version {version}. Use the same '
                'ProxyStore version for the client and endpoint.'
            )
            writer.write(pack_preamble())
            await _send(writer, Status.PROTOCOL_MISMATCH, {'error': error})
            return False

        header, meta = await _read_message(reader)
        if header.code != Op.HELLO:
            raise EndpointProtocolError(
                f'Expected HELLO message but got op {header.code}.',
            )
        client_nonce = _decode_hex(meta, 'nonce')

        server_nonce = os.urandom(NONCE_SIZE)
        proof = compute_proof(self.token, 'server', server_nonce, client_nonce)
        writer.write(pack_preamble())
        await _send(
            writer,
            Status.OK,
            {'nonce': server_nonce.hex(), 'proof': proof.hex()},
        )

        header, meta = await _read_message(reader)
        if header.code != Op.AUTH:
            raise EndpointProtocolError(
                f'Expected AUTH message but got op {header.code}.',
            )
        client_proof = _decode_hex(meta, 'proof')
        if not verify_proof(
            self.token,
            'client',
            client_nonce,
            server_nonce,
            client_proof,
        ):
            logger.warning(
                f'Rejecting connection from {peer} because the client '
                'failed authentication',
            )
            await _send(
                writer, Status.UNAUTHORIZED, {'error': 'invalid token'}
            )
            return False

        info = {
            'uuid': str(self.endpoint.uuid),
            'name': self.endpoint.name,
            'max_object_size': self.max_object_size,
            **local_versions(),
        }
        await _send(writer, Status.OK, info)
        return True

    async def _serve_requests(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        while True:
            try:
                header_bytes = await reader.readexactly(HEADER.size)
            except asyncio.IncompleteReadError:
                # Client closed the connection between requests.
                return
            header = unpack_header(header_bytes)
            meta = decode_meta(await reader.readexactly(header.meta_len))

            if (
                self.max_object_size is not None
                and header.data_len > self.max_object_size
            ):
                # The connection is closed after responding because the
                # client is still sending data we do not want to read.
                error = (
                    f'Data size ({header.data_len} bytes) exceeds the maximum '
                    f'object size of the endpoint ({self.max_object_size} '
                    'bytes).'
                )
                await _send(writer, Status.TOO_LARGE, {'error': error})
                return

            data = (
                await reader.readexactly(header.data_len)
                if header.data_len > 0
                else b''
            )
            status, response_meta, response_data = await self._handle_request(
                header,
                meta,
                data,
            )
            await _send(writer, status, response_meta, response_data)

    async def _handle_request(
        self,
        header: Header,
        meta: dict[str, Any],
        data: bytes,
    ) -> _Response:
        try:
            key, endpoint_uuid = _parse_request(meta)
        except ValueError as e:
            return Status.BAD_REQUEST, {'error': str(e)}, None

        try:
            return await self._dispatch(header.code, key, endpoint_uuid, data)
        except PeerRequestError as e:
            return Status.ERROR, {'error': str(e)}, None
        except ObjectSizeExceededError as e:
            return Status.TOO_LARGE, {'error': str(e)}, None
        except Exception as e:
            logger.exception(
                f'Unexpected error handling {header.code} request'
            )
            return Status.ERROR, {'error': f'unexpected error: {e!r}'}, None

    async def _dispatch(
        self,
        op: int,
        key: str,
        endpoint_uuid: uuid.UUID | None,
        data: bytes,
    ) -> _Response:
        if op == Op.GET:
            result = await self.endpoint.get(key, endpoint=endpoint_uuid)
            if result is None:
                return Status.NOT_FOUND, None, None
            return Status.OK, None, result
        elif op == Op.SET:
            if len(data) == 0:
                error = 'received empty payload'
                return Status.BAD_REQUEST, {'error': error}, None
            await self.endpoint.set(key, data, endpoint=endpoint_uuid)
            return Status.OK, None, None
        elif op == Op.EXISTS:
            exists = await self.endpoint.exists(key, endpoint=endpoint_uuid)
            return Status.OK, {'exists': exists}, None
        elif op == Op.EVICT:
            await self.endpoint.evict(key, endpoint=endpoint_uuid)
            return Status.OK, None, None
        else:
            return Status.BAD_REQUEST, {'error': f'unknown op {op}'}, None


def _parse_request(meta: dict[str, Any]) -> tuple[str, uuid.UUID | None]:
    """Parse the key and optional target endpoint UUID of a request.

    Raises:
        ValueError: If the key is missing or the endpoint UUID is invalid.
    """
    key = meta.get('key')
    if not isinstance(key, str) or len(key) == 0:
        raise ValueError('request missing key')

    endpoint_str = meta.get('endpoint')
    if endpoint_str is None:
        return key, None
    try:
        return key, uuid.UUID(endpoint_str, version=4)
    except (AttributeError, TypeError, ValueError):
        raise ValueError(f'{endpoint_str} is not a valid UUID4') from None


async def _read_message(
    reader: asyncio.StreamReader,
) -> tuple[Header, dict[str, Any]]:
    header = unpack_header(await reader.readexactly(HEADER.size))
    meta = decode_meta(await reader.readexactly(header.meta_len))
    return header, meta


async def _send(
    writer: asyncio.StreamWriter,
    status: Status,
    meta: dict[str, Any] | None = None,
    data: bytes | None = None,
) -> None:
    data_len = 0 if data is None else len(data)
    writer.write(pack_message(status, meta, data_len))
    if data is not None:
        writer.write(data)
    await writer.drain()


def _decode_hex(meta: dict[str, Any], field: str) -> bytes:
    try:
        return bytes.fromhex(meta[field])
    except (KeyError, TypeError, ValueError) as e:
        raise EndpointProtocolError(
            f'Handshake message has missing or invalid {field!r} field.',
        ) from e


def _get_auth_headers(
    method: Literal['globus'] | None,
    **kwargs: Any,
) -> dict[str, str]:
    if method is None:
        return {}
    elif method == 'globus':
        app = get_globus_app()
        scopes = get_relay_scopes_by_resource_server()
        assert len(scopes) == 1
        app.add_scope_requirements(scopes)
        logger.info('Initialized Globus app')
        try:
            authorizer = app.get_authorizer(*scopes.keys())
        except TokenValidationError:
            logger.exception(
                'Failed to find valid tokens for the specified relay '
                'resource server. Have you logged in yet? If not, login then '
                'try again.\n  $ proxystore-globus-auth login',
            )
            raise SystemExit(1) from None
        bearer = authorizer.get_authorization_header()
        assert bearer is not None
        return {'Authorization': bearer}
    else:
        raise AssertionError('Unreachable.')


async def _serve_async(
    config: EndpointConfig,
    endpoint_dir: str,
    stop: asyncio.Event | None = None,
) -> None:
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
    nat_check: asyncio.Task[None] | None = None
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
        ice_servers = (
            None
            if config.relay.ice_servers is None
            else [
                RTCIceServer(
                    urls=server.urls,
                    username=server.username,
                    credential=server.credential,
                )
                for server in config.relay.ice_servers
            ]
        )
        peer_manager = PeerManager(
            relay_client,
            peer_channels=config.relay.peer_channels,
            ice_servers=ice_servers,
        )
        # The NAT check only produces diagnostic logs so it is run
        # concurrently rather than delaying the endpoint from serving
        # requests on networks where STUN is slow or blocked.
        nat_check = asyncio.create_task(check_nat_and_log())

    endpoint = await Endpoint(
        name=config.name,
        uuid=uuid.UUID(config.uuid),
        peer_manager=peer_manager,
        storage=storage,
    )

    loop = asyncio.get_running_loop()
    stop = asyncio.Event() if stop is None else stop
    signals = (signal.SIGINT, signal.SIGTERM)
    for sig in signals:
        loop.add_signal_handler(sig, stop.set)

    token_file = get_token_filepath(endpoint_dir)
    handler = EndpointServer(
        endpoint,
        generate_token_file(token_file),
        max_object_size=config.storage.max_object_size,
    )
    server: asyncio.Server | None = None

    try:
        server = await asyncio.start_server(
            handler.handle_connection,
            host=config.host,
            port=config.port,
        )
        logger.info(
            f'Serving endpoint {uuid.UUID(config.uuid)} ({config.name}) on '
            f'{config.host}:{config.port}',
        )
        logger.info(f'Config: {config}')
        await stop.wait()
        logger.info('Shutting down endpoint server')
    finally:
        for sig in signals:
            loop.remove_signal_handler(sig)
        if server is not None:
            server.close()
            handler.close_connections()
            await server.wait_closed()
        if nat_check is not None and not nat_check.done():
            nat_check.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await nat_check
        await endpoint.close()
        with contextlib.suppress(FileNotFoundError):
            os.remove(token_file)


def serve(
    config: EndpointConfig,
    *,
    endpoint_dir: str,
    log_level: int | str = logging.INFO,
    log_file: str | None = None,
    use_uvloop: bool = True,
) -> None:
    """Initialize and serve an endpoint.

    Warning:
        This function does not return until the server receives SIGINT or
        SIGTERM.

    Args:
        config: Configuration object.
        endpoint_dir: Directory of the endpoint. The client token file is
            written to this directory while the endpoint is running.
        log_level: Logging level of endpoint.
        log_file: Optional file path to append log to.
        use_uvloop: Use uvloop as the event loop implementation.
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

    # The remaining set up and serving code is deferred to within the
    # _serve_async helper function which will be executed within an event loop.
    try:
        if use_uvloop:  # pragma: no cover
            logger.info('Using uvloop as the event loop')
            uvloop.run(_serve_async(config, endpoint_dir))
        else:
            asyncio.run(_serve_async(config, endpoint_dir))
    except Exception as e:
        # Intercept exception so we can log it in the case that the endpoint
        # is running as a daemon process. Otherwise the user will never see
        # the exception.
        logger.exception(f'Caught unhandled exception: {e!r}')
        raise
    except KeyboardInterrupt:  # pragma: no cover
        # SIGINT is handled by _serve_async once the server is running, but
        # can still be raised if received during start up.
        pass
    finally:
        logger.info(f'Finished serving endpoint: {config.name}')
