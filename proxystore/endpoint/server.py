"""Server that handles client connections to an endpoint.

Clients connect to their local endpoint over TCP using the protocol defined
in [`proxystore.endpoint.protocol`][proxystore.endpoint.protocol]. The
[`ClientHandler`][proxystore.endpoint.server.ClientHandler] authenticates
clients and forwards their requests to an
[`Endpoint`][proxystore.endpoint.endpoint.Endpoint].
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import ssl
from collections.abc import Callable
from collections.abc import Coroutine
from typing import Any
from typing import cast
from typing import Protocol
from uuid import UUID

from proxystore.endpoint.auth import compute_proof
from proxystore.endpoint.auth import verify_proof
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.exceptions import ObjectSizeExceededError
from proxystore.endpoint.exceptions import PeerRequestError
from proxystore.endpoint.protocol import Auth
from proxystore.endpoint.protocol import Challenge
from proxystore.endpoint.protocol import decode_meta
from proxystore.endpoint.protocol import EndpointInfo
from proxystore.endpoint.protocol import Header
from proxystore.endpoint.protocol import Hello
from proxystore.endpoint.protocol import NONCE_SIZE
from proxystore.endpoint.protocol import Op
from proxystore.endpoint.protocol import pack_message
from proxystore.endpoint.protocol import Preamble
from proxystore.endpoint.protocol import PROTOCOL_VERSION
from proxystore.endpoint.protocol import Request
from proxystore.endpoint.protocol import Status
from proxystore.endpoint.protocol import VERSION_DOCS_URL
from proxystore.endpoint.protocol import Versions

logger = logging.getLogger(__name__)

_HTTP_METHODS = (b'GET ', b'POST', b'HEAD', b'PUT ')
"""Leading bytes of HTTP requests sent by clients using the old HTTP API."""

HANDSHAKE_TIMEOUT = 10
"""Seconds a client has to complete the handshake after connecting."""

_Response = tuple[Status, dict[str, Any] | None, bytes | bytearray | None]


class _ClientConnection(asyncio.BufferedProtocol):
    """Client connection that receives data directly into buffers.

    This provides the subset of the
    [`StreamReader`][asyncio.StreamReader] and
    [`StreamWriter`][asyncio.StreamWriter] interfaces used by the
    [`ClientHandler`][proxystore.endpoint.server.ClientHandler].
    Unlike [`StreamReader.readexactly()`][asyncio.StreamReader.readexactly],
    which copies received data through an internal buffer, `readexactly()`
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
        callback: Callable[[_ClientConnection], Coroutine[Any, Any, None]],
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

        self._pending += memoryview(self._spare)[:nbytes]
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

    def can_write_eof(self) -> bool:
        """Check if the transport supports closing only the write side."""
        assert self._transport is not None
        return self._transport.can_write_eof()

    def write_eof(self) -> None:
        """Close the write side of the connection."""
        assert self._transport is not None
        self._transport.write_eof()

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


class EndpointBackend(Protocol):
    """Endpoint that client requests are forwarded to.

    [`Endpoint`][proxystore.endpoint.endpoint.Endpoint] implements this
    protocol.
    """

    @property
    def uuid(self) -> UUID:
        """UUID of the endpoint."""
        ...

    @property
    def name(self) -> str:
        """Name of the endpoint."""
        ...

    async def evict(self, key: str, endpoint: UUID | None = None) -> None:
        """Evict the object associated with the key."""
        ...

    async def exists(
        self,
        key: str,
        endpoint: UUID | None = None,
    ) -> bool:
        """Check if an object associated with the key exists."""
        ...

    async def get(
        self,
        key: str,
        endpoint: UUID | None = None,
    ) -> bytes | bytearray | None:
        """Get the object associated with the key."""
        ...

    async def set(
        self,
        key: str,
        data: bytes | bytearray,
        endpoint: UUID | None = None,
    ) -> None:
        """Set the object associated with the key."""
        ...


class ClientHandler:
    """Handles client connections to an endpoint.

    The handler authenticates each client with the handshake defined in
    [`proxystore.endpoint.protocol`][proxystore.endpoint.protocol] then
    forwards the client's requests to the endpoint.

    Example:
        ```python
        handler = ClientHandler(endpoint, token)
        server = await handler.start_server('localhost', 8765)
        ...
        server.close()
        handler.close_connections()
        await server.wait_closed()
        ```

    Args:
        endpoint: Endpoint to forward client requests to.
        token: Token that clients must prove they know.
        max_object_size: Optional maximum size in bytes of objects that
            clients can set. Requests exceeding this size are rejected
            before the data is read. This should match the maximum object
            size of the endpoint's storage, which rejects objects only after
            the data is read.
        handshake_timeout: Seconds a client has to complete the handshake.
    """

    def __init__(
        self,
        endpoint: EndpointBackend,
        token: bytes,
        *,
        max_object_size: int | None = None,
        handshake_timeout: float = HANDSHAKE_TIMEOUT,
    ) -> None:
        self.endpoint = endpoint
        self.token = token
        self.max_object_size = max_object_size
        self.handshake_timeout = handshake_timeout
        self._connections: set[_ClientConnection] = set()
        self._warned_versions: set[Versions] = set()

    async def start_server(
        self,
        host: str,
        port: int,
        *,
        ssl_context: ssl.SSLContext | None = None,
    ) -> asyncio.Server:
        """Start a server that handles connections on the host and port.

        Args:
            host: Address to listen on.
            port: Port to listen on.
            ssl_context: Optional SSL context to encrypt connections with TLS.
        """
        loop = asyncio.get_running_loop()
        return await loop.create_server(
            lambda: _ClientConnection(self._handle_connection),
            host=host,
            port=port,
            ssl=ssl_context,
        )

    def close_connections(self) -> None:
        """Close all open client connections."""
        for conn in list(self._connections):
            conn.close()

    async def _handle_connection(self, conn: _ClientConnection) -> None:
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

    async def _handshake(self, conn: _ClientConnection, peer: Any) -> bool:
        preamble = await conn.readexactly(Preamble.SIZE)
        if bytes(preamble[:4]) in _HTTP_METHODS:
            logger.warning(
                f'Rejecting HTTP request from {peer}. The client is likely '
                'using an older version of ProxyStore that uses the HTTP API.',
            )
            await _reply_and_close(conn, _http_upgrade_response())
            return False

        version = Preamble.unpack(preamble).version
        if version != PROTOCOL_VERSION:
            logger.warning(
                f'Rejecting connection from {peer} with protocol version '
                f'{version} (expected {PROTOCOL_VERSION})',
            )
            # Only the preamble format is the same across protocol versions
            # so the client detects the mismatch from our preamble.
            await _reply_and_close(conn, Preamble().pack())
            return False

        hello = Hello.from_meta(await _read_handshake_message(conn, Op.HELLO))
        nonce = os.urandom(NONCE_SIZE)
        challenge = Challenge(
            nonce=nonce,
            proof=compute_proof(self.token, 'server', nonce, hello.nonce),
        )
        conn.write(Preamble().pack())
        await _send(conn, Status.OK, challenge.to_meta())

        auth = Auth.from_meta(await _read_handshake_message(conn, Op.AUTH))
        if not verify_proof(
            self.token,
            'client',
            hello.nonce,
            challenge.nonce,
            auth.proof,
        ):
            logger.warning(
                f'Rejecting connection from {peer} because the client '
                'failed authentication',
            )
            await _send(conn, Status.UNAUTHORIZED, {'error': 'invalid token'})
            return False

        self._check_client_versions(peer, hello.versions)
        info = EndpointInfo(
            uuid=self.endpoint.uuid,
            name=self.endpoint.name,
            versions=Versions.current(),
            max_object_size=self.max_object_size,
        )
        await _send(conn, Status.OK, info.to_meta())
        return True

    def _check_client_versions(self, peer: Any, versions: Versions) -> None:
        mismatches = versions.mismatches(Versions.current())
        if len(mismatches) > 0 and versions not in self._warned_versions:
            # Only warn once for each combination of client versions.
            self._warned_versions.add(versions)
            logger.warning(
                f'Client {peer} uses different versions than this endpoint: '
                f'{"; ".join(mismatches)}. Objects serialized in one '
                'environment may fail to deserialize in another. See '
                f'{VERSION_DOCS_URL} for details.',
            )

    async def _serve_requests(self, conn: _ClientConnection) -> None:
        while True:
            try:
                header_bytes = await conn.readexactly(Header.SIZE)
            except asyncio.IncompleteReadError:
                # Client closed the connection between requests.
                return
            header = Header.unpack(header_bytes)
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
                header.code,
                meta,
                data,
            )
            await _send(conn, status, response_meta, response_data)

    async def _handle_request(
        self,
        op: int,
        meta: dict[str, Any],
        data: bytes | bytearray,
    ) -> _Response:
        try:
            request = Request.from_meta(meta)
        except EndpointProtocolError as e:
            return Status.BAD_REQUEST, {'error': str(e)}, None

        try:
            return await self._dispatch(op, request, data)
        except PeerRequestError as e:
            return Status.ERROR, {'error': str(e)}, None
        except ObjectSizeExceededError as e:
            return Status.TOO_LARGE, {'error': str(e)}, None
        except Exception as e:
            logger.exception(f'Unexpected error handling op {op} request')
            return Status.ERROR, {'error': f'unexpected error: {e!r}'}, None

    async def _dispatch(
        self,
        op: int,
        request: Request,
        data: bytes | bytearray,
    ) -> _Response:
        key, endpoint_uuid = request.key, request.endpoint
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


async def _read_handshake_message(
    conn: _ClientConnection,
    expected: Op,
) -> dict[str, Any]:
    header = Header.unpack(await conn.readexactly(Header.SIZE))
    if header.code != expected:
        raise EndpointProtocolError(
            f'Expected {expected.name} message but got op {header.code}.',
        )
    elif header.data_len != 0:
        raise EndpointProtocolError(
            f'Client sent data in a {expected.name} message.',
        )
    return decode_meta(await conn.readexactly(header.meta_len))


async def _send(
    conn: _ClientConnection,
    status: Status,
    meta: dict[str, Any] | None = None,
    data: bytes | bytearray | None = None,
) -> None:
    data_len = 0 if data is None else len(data)
    conn.write(pack_message(status, meta, data_len))
    if data is not None:
        conn.write(data)
    await conn.drain()


async def _reply_and_close(conn: _ClientConnection, data: bytes) -> None:
    conn.write(data)
    await conn.drain()
    # Closing the connection while an unread request is still in the
    # receive buffer causes the OS to reset the connection so the client
    # may never read the reply. Instead, only close our side and give the
    # client time to read the reply and close the connection.
    if conn.can_write_eof():  # pragma: no branch
        conn.write_eof()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(conn.wait_closed(), timeout=1)


def _http_upgrade_response() -> bytes:
    version = Versions.current().proxystore
    body = (
        f'This endpoint uses ProxyStore {version} which no longer supports '
        'the HTTP API used by older versions of ProxyStore. Upgrade '
        'ProxyStore on the client to the same version as the endpoint. See '
        f'{VERSION_DOCS_URL} for details.\n'
    ).encode()
    headers = (
        'HTTP/1.1 426 Upgrade Required\r\n'
        'Content-Type: text/plain; charset=utf-8\r\n'
        f'Content-Length: {len(body)}\r\n'
        'Connection: close\r\n'
        '\r\n'
    ).encode()
    return headers + body
