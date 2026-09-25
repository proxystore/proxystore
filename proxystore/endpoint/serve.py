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
from collections.abc import Callable
from collections.abc import Coroutine
from typing import Any
from typing import cast
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

_Response = tuple[Status, dict[str, Any] | None, bytes | bytearray | None]


class ClientConnection(asyncio.BufferedProtocol):
    """Client connection that receives data directly into buffers.

    This provides the subset of the
    [`StreamReader`][asyncio.StreamReader] and
    [`StreamWriter`][asyncio.StreamWriter] interfaces used by the
    [`EndpointServer`][proxystore.endpoint.serve.EndpointServer].
    Unlike [`StreamReader.readexactly()`][asyncio.StreamReader.readexactly],
    which copies received data through an internal buffer,
    [`readexactly()`][proxystore.endpoint.serve.ClientConnection.readexactly]
    has the transport write directly into the returned buffer. This roughly
    triples the throughput of receiving large objects.

    Args:
        callback: Coroutine function called with this connection once the
            connection is made.
    """

    _SPARE_SIZE = 64 * 1024
    _MAX_PENDING_SIZE = 1024 * 1024

    def __init__(
        self,
        callback: Callable[[ClientConnection], Coroutine[Any, Any, None]],
    ) -> None:
        self._callback = callback
        self._transport: asyncio.Transport | None = None
        self._task: asyncio.Task[None] | None = None
        loop = asyncio.get_running_loop()
        self._closed: asyncio.Future[None] = loop.create_future()

        # Data received while no read is waiting.
        self._spare = bytearray(self._SPARE_SIZE)
        self._pending = bytearray()
        # Buffer of the read currently waiting for data.
        self._target: memoryview | None = None
        self._target_pos = 0
        self._read_waiter: asyncio.Future[None] | None = None
        self._reading_paused = False
        self._eof = False

        self._writing_paused = False
        self._drain_waiter: asyncio.Future[None] | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """Start the callback task for the new connection."""
        # uvloop transports do not subclass asyncio.Transport but implement
        # the same interface.
        self._transport = cast(asyncio.Transport, transport)
        self._task = asyncio.get_running_loop().create_task(
            self._callback(self),
        )

    def get_buffer(self, sizehint: int) -> memoryview:
        """Get the buffer that the transport should write data into."""
        if self._target is not None:
            return self._target[self._target_pos :]
        return memoryview(self._spare)

    def buffer_updated(self, nbytes: int) -> None:
        """Process data written into the buffer from `get_buffer()`."""
        if self._target is not None:
            self._target_pos += nbytes
            if self._target_pos == len(self._target):
                # Stop receiving into the target immediately because the
                # transport may request another buffer before the reader
                # wakes up, and the buffer must not be empty.
                self._target = None
                self._wake_reader()
            return

        self._pending += self._spare[:nbytes]
        if (
            len(self._pending) > self._MAX_PENDING_SIZE
            and not self._reading_paused
        ):
            assert self._transport is not None
            self._transport.pause_reading()
            self._reading_paused = True

    def eof_received(self) -> bool:
        """Handle the client closing its side of the connection."""
        self._eof = True
        self._wake_reader()
        return False

    def connection_lost(self, exc: Exception | None) -> None:
        """Handle the connection being closed."""
        self._eof = True
        self._wake_reader()
        if self._drain_waiter is not None and not self._drain_waiter.done():
            self._drain_waiter.set_exception(
                ConnectionResetError('Connection lost'),
            )
        if not self._closed.done():  # pragma: no branch
            self._closed.set_result(None)

    def pause_writing(self) -> None:
        """Pause writing when the transport's write buffer is full."""
        self._writing_paused = True

    def resume_writing(self) -> None:
        """Resume writing when the transport's write buffer drains."""
        self._writing_paused = False
        if self._drain_waiter is not None and not self._drain_waiter.done():
            self._drain_waiter.set_result(None)

    def get_extra_info(self, name: str) -> Any:
        """Get information about the transport."""
        assert self._transport is not None
        return self._transport.get_extra_info(name)

    async def readexactly(self, n: int) -> bytearray:
        """Read exactly `n` bytes.

        Raises:
            IncompleteReadError: If the connection is closed before `n`
                bytes are read.
        """
        buffer = bytearray(n)
        view = memoryview(buffer)
        received = min(n, len(self._pending))
        view[:received] = self._pending[:received]
        del self._pending[:received]
        if self._reading_paused and len(self._pending) == 0:
            assert self._transport is not None
            self._transport.resume_reading()
            self._reading_paused = False

        if received < n:
            if self._eof:
                raise asyncio.IncompleteReadError(bytes(view[:received]), n)
            self._target = view
            self._target_pos = received
            self._read_waiter = asyncio.get_running_loop().create_future()
            try:
                await self._read_waiter
            finally:
                received = self._target_pos
                self._target = None
                self._read_waiter = None
            if received < n:
                raise asyncio.IncompleteReadError(bytes(view[:received]), n)
        return buffer

    def write(self, data: bytes | bytearray | memoryview) -> None:
        """Write data to the connection."""
        assert self._transport is not None
        self._transport.write(data)

    async def drain(self) -> None:
        """Wait until it is appropriate to write more data.

        Raises:
            ConnectionResetError: If the connection is closed.
        """
        assert self._transport is not None
        if self._transport.is_closing():
            raise ConnectionResetError('Connection lost')
        if self._writing_paused:
            self._drain_waiter = asyncio.get_running_loop().create_future()
            try:
                await self._drain_waiter
            finally:
                self._drain_waiter = None

    def close(self) -> None:
        """Close the connection."""
        assert self._transport is not None
        self._transport.close()

    async def wait_closed(self) -> None:
        """Wait until the connection is closed."""
        await asyncio.shield(self._closed)

    def _wake_reader(self) -> None:
        if self._read_waiter is not None and not self._read_waiter.done():
            self._read_waiter.set_result(None)


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
        self._connections: set[ClientConnection] = set()

    async def start_server(self, host: str, port: int) -> asyncio.Server:
        """Start a server that handles connections on the host and port."""
        loop = asyncio.get_running_loop()
        return await loop.create_server(
            lambda: ClientConnection(self.handle_connection),
            host=host,
            port=port,
        )

    def close_connections(self) -> None:
        """Close all open client connections."""
        for conn in list(self._connections):
            conn.close()

    async def handle_connection(self, conn: ClientConnection) -> None:
        """Handle a client connection until it is closed."""
        self._connections.add(conn)
        peer = conn.get_extra_info('peername')
        sock = conn.get_extra_info('socket')
        if sock is not None:  # pragma: no branch
            with contextlib.suppress(OSError):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        try:
            try:
                authenticated = await asyncio.wait_for(
                    self._handshake(conn, peer),
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
                await self._serve_requests(conn)
        except (
            ConnectionError,
            asyncio.IncompleteReadError,
            EndpointProtocolError,
        ) as e:
            logger.debug(f'Closing connection from {peer}: {e!r}')
        finally:
            self._connections.discard(conn)
            conn.close()
            await conn.wait_closed()

    async def _handshake(self, conn: ClientConnection, peer: Any) -> bool:
        version = unpack_preamble(await conn.readexactly(PREAMBLE.size))
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
            conn.write(pack_preamble())
            await _send(conn, Status.PROTOCOL_MISMATCH, {'error': error})
            return False

        header, meta = await _read_message(conn)
        if header.code != Op.HELLO:
            raise EndpointProtocolError(
                f'Expected HELLO message but got op {header.code}.',
            )
        client_nonce = _decode_hex(meta, 'nonce')

        server_nonce = os.urandom(NONCE_SIZE)
        proof = compute_proof(self.token, 'server', server_nonce, client_nonce)
        conn.write(pack_preamble())
        await _send(
            conn,
            Status.OK,
            {'nonce': server_nonce.hex(), 'proof': proof.hex()},
        )

        header, meta = await _read_message(conn)
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
            await _send(conn, Status.UNAUTHORIZED, {'error': 'invalid token'})
            return False

        info = {
            'uuid': str(self.endpoint.uuid),
            'name': self.endpoint.name,
            'max_object_size': self.max_object_size,
            **local_versions(),
        }
        await _send(conn, Status.OK, info)
        return True

    async def _serve_requests(self, conn: ClientConnection) -> None:
        while True:
            try:
                header_bytes = await conn.readexactly(HEADER.size)
            except asyncio.IncompleteReadError:
                # Client closed the connection between requests.
                return
            header = unpack_header(header_bytes)
            meta = decode_meta(await conn.readexactly(header.meta_len))

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
                await _send(conn, Status.TOO_LARGE, {'error': error})
                return

            data = (
                await conn.readexactly(header.data_len)
                if header.data_len > 0
                else b''
            )
            status, response_meta, response_data = await self._handle_request(
                header,
                meta,
                data,
            )
            await _send(conn, status, response_meta, response_data)

    async def _handle_request(
        self,
        header: Header,
        meta: dict[str, Any],
        data: bytes | bytearray,
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
        data: bytes | bytearray,
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
    conn: ClientConnection,
) -> tuple[Header, dict[str, Any]]:
    header = unpack_header(await conn.readexactly(HEADER.size))
    meta = decode_meta(await conn.readexactly(header.meta_len))
    return header, meta


async def _send(
    conn: ClientConnection,
    status: Status,
    meta: dict[str, Any] | None = None,
    data: bytes | bytearray | None = None,
) -> None:
    data_len = 0 if data is None else len(data)
    conn.write(pack_message(status, meta, data_len))
    if data is not None:
        conn.write(data)
    await conn.drain()


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
        server = await handler.start_server(config.host, config.port)
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
