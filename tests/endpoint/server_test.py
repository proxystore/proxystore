from __future__ import annotations

import asyncio
import logging
import os
import pathlib
import socket
import ssl
from collections.abc import AsyncGenerator
from typing import Any
from typing import NamedTuple
from unittest import mock
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from proxystore.endpoint.auth import EndpointToken
from proxystore.endpoint.auth import TLSCertificate
from proxystore.endpoint.client import _recv_exactly
from proxystore.endpoint.client import _recv_message
from proxystore.endpoint.client import EndpointClient
from proxystore.endpoint.dispatch import Dispatcher
from proxystore.endpoint.exceptions import EndpointAuthError
from proxystore.endpoint.exceptions import EndpointConnectionError
from proxystore.endpoint.exceptions import EndpointNotRunningError
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.exceptions import ObjectSizeExceededError
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.protocol import Header
from proxystore.endpoint.protocol import Hello
from proxystore.endpoint.protocol import MAX_META_SIZE
from proxystore.endpoint.protocol import Message
from proxystore.endpoint.protocol import MIN_PROTOCOL_VERSION
from proxystore.endpoint.protocol import Op
from proxystore.endpoint.protocol import PingResult
from proxystore.endpoint.protocol import Preamble
from proxystore.endpoint.protocol import PROTOCOL_VERSION
from proxystore.endpoint.protocol import Request
from proxystore.endpoint.protocol import Status
from proxystore.endpoint.protocol import Versions
from proxystore.endpoint.server import _ClientConnection
from proxystore.endpoint.server import _format_address
from proxystore.endpoint.server import ClientHandler
from proxystore.endpoint.storage import MemoryStorage
from testing.compat import randbytes
from testing.endpoint import decode_meta
from testing.endpoint import encode_meta
from testing.utils import wait_until

MAX_OBJECT_SIZE = 10_000_000


class _Server(NamedTuple):
    handler: ClientHandler
    dispatcher: Dispatcher
    token: EndpointToken
    host: str
    port: int


@pytest_asyncio.fixture()
async def server() -> AsyncGenerator[_Server, None]:
    dispatcher = Dispatcher(EndpointId.random(), MemoryStorage())
    token = EndpointToken.generate()
    handler = ClientHandler(
        dispatcher,
        token,
        name='my-endpoint',
        max_object_size=MAX_OBJECT_SIZE,
        handshake_timeout=1,
    )
    port = await handler.start('127.0.0.1', 0)
    yield _Server(handler, dispatcher, token, '127.0.0.1', port)
    await handler.close()
    await dispatcher.storage.close()


async def _connect(
    server: _Server,
    token: EndpointToken | None = None,
) -> Any:
    token = server.token if token is None else token
    return await asyncio.to_thread(
        EndpointClient.connect,
        server.host,
        server.port,
        token,
    )


def _raw_socket(server: _Server) -> socket.socket:
    return socket.create_connection((server.host, server.port), timeout=5)


def _is_closed(sock: socket.socket) -> bool:
    try:
        return sock.recv(1) == b''
    except ConnectionResetError:  # pragma: no cover
        return True


async def test_operations(server: _Server) -> None:
    client = await _connect(server)
    assert client.info.id == server.dispatcher.id
    assert client.info.name == 'my-endpoint'
    assert client.info.versions == Versions.current()

    small, large = b'value', randbytes(5_000_000)
    await asyncio.to_thread(client.set, 'small', small)
    await asyncio.to_thread(client.set, 'large', large)
    assert await asyncio.to_thread(client.get, 'small') == small
    assert await asyncio.to_thread(client.get, 'large') == large
    assert await asyncio.to_thread(client.exists, 'small')

    # Non-contiguous buffers are copied before sending
    await asyncio.to_thread(client.set, 'strided', memoryview(b'abcdef')[::2])
    assert await asyncio.to_thread(client.get, 'strided') == b'ace'

    # Empty objects are valid
    await asyncio.to_thread(client.set, 'empty', b'')
    assert await asyncio.to_thread(client.get, 'empty') == b''

    await asyncio.to_thread(client.evict, 'small')
    assert not await asyncio.to_thread(client.exists, 'small')
    assert await asyncio.to_thread(client.get, 'small') is None

    await asyncio.to_thread(client.close)


async def test_client_closes_between_requests(server: _Server) -> None:
    client = await _connect(server)
    await asyncio.to_thread(client.exists, 'key')
    (conn,) = server.handler._connections
    await asyncio.to_thread(client.close)

    # The server's connection handler returns once the client disconnects
    assert conn._task is not None
    await asyncio.wait_for(conn._task, timeout=5)
    assert len(server.handler._connections) == 0


async def test_client_wrong_token(server: _Server) -> None:
    # The client detects the server does not know the client's token first
    # (i.e., an impostor endpoint) before sending its own proof.
    with pytest.raises(EndpointAuthError, match='failed to prove'):
        await _connect(server, token=EndpointToken.generate())


def _raw_hello(sock: socket.socket) -> tuple[bytes, dict[str, Any]]:
    client_nonce = os.urandom(32)
    hello = Hello(nonce=client_nonce, versions=Versions.current()).encode()
    sock.sendall(Preamble().pack() + Message(Op.HELLO, hello).pack_head())
    assert (
        Preamble.unpack(bytes(_recv_exactly(sock, Preamble.SIZE))).version == 1
    )
    response = _recv_message(sock)
    assert response.code == Status.OK
    return client_nonce, decode_meta(response.meta)


@pytest.mark.parametrize('replay', (False, True))
async def test_server_rejects_bad_proof(server: _Server, replay: bool) -> None:
    # A proof computed with the wrong token is rejected, and the server's own
    # proof cannot be sent back as the client's proof.
    def _run() -> None:
        with _raw_socket(server) as sock:
            client_nonce, meta = _raw_hello(sock)
            if replay:
                proof = meta['proof']
            else:
                server_nonce = bytes.fromhex(meta['nonce'])
                token = EndpointToken.generate()
                proof = token.proof('client', client_nonce, server_nonce).hex()
            sock.sendall(
                Message(Op.AUTH, encode_meta({'proof': proof})).pack_head()
            )
            assert _recv_message(sock).code == Status.UNAUTHORIZED
            assert _is_closed(sock)

    await asyncio.to_thread(_run)


async def test_protocol_version_unsupported(server: _Server) -> None:
    def _run() -> None:
        with _raw_socket(server) as sock:
            # The HELLO of an unsupported protocol version is never read
            hello = Message(Op.HELLO, encode_meta({'old': 'format'}))
            version = MIN_PROTOCOL_VERSION - 1
            sock.sendall(Preamble(version).pack() + hello.pack_head())
            preamble = _recv_exactly(sock, Preamble.SIZE)
            assert Preamble.unpack(bytes(preamble)).version == PROTOCOL_VERSION
            assert _is_closed(sock)

    await asyncio.to_thread(_run)


async def test_protocol_version_negotiated(server: _Server) -> None:
    def _run() -> None:
        with _raw_socket(server) as sock:
            # A newer client uses the newest version the endpoint supports
            hello = Hello(nonce=os.urandom(32), versions=Versions.current())
            message = Message(Op.HELLO, hello.encode())
            sock.sendall(
                Preamble(PROTOCOL_VERSION + 1).pack() + message.pack_head(),
            )
            preamble = _recv_exactly(sock, Preamble.SIZE)
            assert Preamble.unpack(bytes(preamble)).version == PROTOCOL_VERSION
            assert _recv_message(sock).code == Status.OK

    await asyncio.to_thread(_run)


@pytest.mark.parametrize(
    'message',
    (
        # Bad magic
        b'XXXX\x00\x01',
        # First message is not HELLO
        Preamble().pack()
        + Message(Op.AUTH, encode_meta({'proof': '00'})).pack_head(),
        # HELLO is missing the nonce
        Preamble().pack() + Message(Op.HELLO, encode_meta({})).pack_head(),
        # HELLO has malformed metadata
        Preamble().pack() + Header(Op.HELLO, 0, 0, 2, 0).pack() + b'[]',
        # HELLO nonce is too short
        Preamble().pack()
        + Message(
            Op.HELLO,
            encode_meta(
                {
                    'nonce': os.urandom(16).hex(),
                    'versions': Versions.current().model_dump(),
                },
            ),
        ).pack_head(),
        # HELLO contains data
        Preamble().pack()
        + Message(
            Op.HELLO,
            Hello(nonce=os.urandom(32), versions=Versions.current()).encode(),
        ).pack_head(1)
        + b'x',
    ),
)
async def test_bad_handshake_closes_connection(
    message: bytes,
    server: _Server,
) -> None:
    def _run() -> None:
        with _raw_socket(server) as sock:
            sock.sendall(message)
            assert _is_closed(sock)

    await asyncio.to_thread(_run)


async def test_bad_auth_message_closes_connection(server: _Server) -> None:
    def _run() -> None:
        with _raw_socket(server) as sock:
            _raw_hello(sock)
            sock.sendall(
                Message(Op.GET, encode_meta({'key': 'key'})).pack_head()
            )
            assert _is_closed(sock)

    await asyncio.to_thread(_run)


async def test_handshake_timeout(server: _Server, caplog) -> None:
    def _run() -> None:
        with _raw_socket(server) as sock:
            # Server should close the connection after the handshake timeout
            assert _is_closed(sock)

    await asyncio.to_thread(_run)
    assert any(
        'did not complete the handshake' in r.message for r in caplog.records
    )


async def _raw_request(
    client: EndpointClient,
    message: bytes,
) -> tuple[int, dict[str, Any]]:
    def _run() -> tuple[int, dict[str, Any]]:
        client._socket.sendall(message)
        response = _recv_message(client._socket)
        return response.code, decode_meta(response.meta)

    return await asyncio.to_thread(_run)


async def test_bad_requests(server: _Server) -> None:
    client = await _connect(server)

    code, meta = await _raw_request(
        client, Message(Op.GET, encode_meta({'key': 42})).pack_head()
    )
    assert code == Status.BAD_REQUEST
    assert "invalid 'key'" in meta['error']

    request = Request(key='key').encode()
    code, meta = await _raw_request(client, Message(99, request).pack_head())
    assert code == Status.BAD_REQUEST
    assert 'unknown op' in meta['error']

    code, meta = await _raw_request(
        client,
        Message(
            Op.GET, encode_meta({'key': 'key', 'target': 'not-an-id'})
        ).pack_head(),
    )
    assert code == Status.BAD_REQUEST
    assert "invalid 'target'" in meta['error']

    # The client validates the endpoint ID before sending the request
    with pytest.raises(ValueError, match='not a valid endpoint ID'):
        await asyncio.to_thread(client.get, 'key', 'not-an-id')

    # The connection is still usable after these errors
    assert not await asyncio.to_thread(client.exists, 'key')
    await asyncio.to_thread(client.close)


async def test_request_meta_too_large(server: _Server) -> None:
    client = await _connect(server)

    def _run() -> None:
        header = Header(Op.GET, 0, 0, MAX_META_SIZE + 1, 0).pack()
        client._socket.sendall(header)
        assert _is_closed(client._socket)

    await asyncio.to_thread(_run)
    await asyncio.to_thread(client.close)


async def test_data_too_large(server: _Server) -> None:
    client = await _connect(server)
    assert client.info.max_object_size == MAX_OBJECT_SIZE

    # The client checks the size before sending the data
    data = randbytes(MAX_OBJECT_SIZE + 1)
    with pytest.raises(ObjectSizeExceededError, match='exceeds the maximum'):
        await asyncio.to_thread(client.set, 'key', data)
    assert not client.closed
    await asyncio.to_thread(client.close)


async def test_data_too_large_reply_not_lost(server: _Server) -> None:
    client = await _connect(server)

    def _run() -> tuple[int, dict[str, Any]]:
        # The server checks the size before reading the data. Unread data in
        # the endpoint's receive buffer must not cause the connection to be
        # reset before the client reads the reply.
        message = Message(Op.SET, encode_meta({'key': 'key'})).pack_head(
            MAX_OBJECT_SIZE + 1
        )
        client._socket.sendall(message + randbytes(1_000_000))
        response = _recv_message(client._socket)
        # The connection is closed because the data was not read
        assert _is_closed(client._socket)
        return response.code, decode_meta(response.meta)

    for _ in range(10):
        code, meta = await asyncio.to_thread(_run)
        assert code == Status.TOO_LARGE
        assert 'exceeds the maximum' in meta['error']
        await asyncio.to_thread(client.close)
        client = await _connect(server)
    await asyncio.to_thread(client.close)


async def test_close(server: _Server) -> None:
    client = await _connect(server)
    await server.handler.close()
    with pytest.raises(EndpointConnectionError):
        await asyncio.to_thread(client.exists, 'key')
    assert client.closed
    # New connections are refused and closing again is a no-op
    with pytest.raises(EndpointNotRunningError):
        await _connect(server)
    await server.handler.close()


async def test_start_twice(server: _Server) -> None:
    with pytest.raises(RuntimeError, match='already been started'):
        await server.handler.start('127.0.0.1', 0)


async def test_close_cancels_requests(server: _Server) -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def _never_finishes(*args: Any, **kwargs: Any) -> None:
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    client = await _connect(server)
    with mock.patch.object(
        server.dispatcher.storage,
        'exists',
        _never_finishes,
    ):
        request = asyncio.create_task(asyncio.to_thread(client.exists, 'key'))
        await started.wait()
        await server.handler.close(timeout=0.1)
        assert cancelled.is_set()
        assert len(server.handler._tasks) == 0
        with pytest.raises(EndpointConnectionError):
            await request


async def test_unexpected_connection_error_logged(
    server: _Server,
    caplog,
) -> None:
    with mock.patch.object(
        server.handler,
        '_handshake',
        AsyncMock(side_effect=RuntimeError('oops')),
    ):
        client_error = asyncio.to_thread(_connect_sync, server)
        with pytest.raises(EndpointConnectionError):
            await client_error
    assert any('Unexpected error' in r.message for r in caplog.records)


def _connect_sync(server: _Server) -> EndpointClient:
    return EndpointClient.connect(server.host, server.port, server.token)


class _FakeTransport:
    def __init__(self) -> None:
        self.protocol: _ClientConnection | None = None
        self.written = bytearray()
        self.reading_paused = False
        self.closing = False

    def write(self, data: bytes) -> None:
        self.written += data

    def pause_reading(self) -> None:
        self.reading_paused = True

    def resume_reading(self) -> None:
        self.reading_paused = False

    def is_closing(self) -> bool:
        return self.closing

    def close(self) -> None:
        self.closing = True
        assert self.protocol is not None
        self.protocol.connection_lost(None)

    def get_extra_info(self, name: str) -> Any:
        return f'extra-{name}'


async def _fake_connection() -> tuple[_ClientConnection, _FakeTransport]:
    conn = _ClientConnection(AsyncMock(), set())
    transport = _FakeTransport()
    transport.protocol = conn
    conn.connection_made(transport)  # type: ignore[arg-type]
    await asyncio.sleep(0)
    return conn, transport


def _feed(conn: _ClientConnection, data: bytes) -> None:
    view = memoryview(data)
    while len(view) > 0:
        buffer = conn.get_buffer(-1)
        n = min(len(buffer), len(view))
        buffer[:n] = view[:n]
        conn.buffer_updated(n)
        view = view[n:]


async def test_connection_pending_data_flow_control() -> None:
    conn, transport = await _fake_connection()
    assert conn.get_extra_info('peername') == 'extra-peername'

    # Data received while no read is waiting is buffered until too much
    # is buffered, then reading is paused.
    data = randbytes(_ClientConnection._MAX_PENDING_SIZE + 1)
    _feed(conn, data)
    assert transport.reading_paused

    assert await conn.readexactly(len(data)) == data
    assert not transport.reading_paused


async def test_connection_discard_incoming() -> None:
    conn, transport = await _fake_connection()
    _feed(conn, randbytes(_ClientConnection._MAX_PENDING_SIZE + 1))
    assert transport.reading_paused

    # Discarding resumes reading and drops pending and future data
    conn.discard_incoming()
    assert not transport.reading_paused
    _feed(conn, randbytes(_ClientConnection._MAX_PENDING_SIZE + 1))
    assert not transport.reading_paused
    assert len(conn._pending) == 0


async def test_connection_read_split_across_buffers() -> None:
    conn, _ = await _fake_connection()
    _feed(conn, b'abc')
    task = asyncio.create_task(conn.readexactly(6))
    await asyncio.sleep(0)
    _feed(conn, b'defgh')
    assert await task == b'abcdef'
    assert await conn.readexactly(2) == b'gh'


async def test_connection_eof_before_read() -> None:
    conn, _ = await _fake_connection()
    _feed(conn, b'abc')
    conn.eof_received()
    with pytest.raises(asyncio.IncompleteReadError) as exc_info:
        await conn.readexactly(5)
    assert exc_info.value.partial == b'abc'


async def test_connection_eof_during_read() -> None:
    conn, _ = await _fake_connection()
    task = asyncio.create_task(conn.readexactly(10))
    await asyncio.sleep(0)
    _feed(conn, b'abcd')
    conn.eof_received()
    with pytest.raises(asyncio.IncompleteReadError) as exc_info:
        await task
    assert exc_info.value.partial == b'abcd'


async def test_connection_write_after_close() -> None:
    conn, transport = await _fake_connection()
    transport.close()
    with pytest.raises(ConnectionResetError):
        conn.write(b'data')
    assert transport.written == b''


async def test_connection_drain() -> None:
    conn, transport = await _fake_connection()
    conn.write(b'data')
    assert transport.written == b'data'
    await conn.drain()

    # Drain waits until writing is resumed
    conn.pause_writing()
    task = asyncio.create_task(conn.drain())
    await asyncio.sleep(0)
    assert not task.done()
    conn.resume_writing()
    await task

    # Resuming without a drain waiting is a no-op
    conn.resume_writing()

    # Drain fails if the connection is lost while waiting
    conn.pause_writing()
    task = asyncio.create_task(conn.drain())
    await asyncio.sleep(0)
    conn.close()
    with pytest.raises(ConnectionResetError):
        await task
    await conn.wait_closed()

    # Drain fails if the connection is already closing
    with pytest.raises(ConnectionResetError):
        await conn.drain()


async def test_http_request_rejected(server: _Server, caplog) -> None:
    def _run() -> bytes:
        with _raw_socket(server) as sock:
            sock.sendall(b'GET /get?key=abc HTTP/1.1\r\nHost: x\r\n\r\n')
            response = bytearray()
            while chunk := sock.recv(65536):
                response += chunk
            return bytes(response)

    response = await asyncio.to_thread(_run)
    assert response.startswith(b'HTTP/1.1 426 Upgrade Required\r\n')
    assert b'Upgrade ProxyStore on the client' in response
    assert any('Rejecting HTTP request' in r.message for r in caplog.records)


def _raw_handshake(server: _Server, versions: Versions) -> None:
    with _raw_socket(server) as sock:
        client_nonce = os.urandom(32)
        hello = Hello(nonce=client_nonce, versions=versions).encode()
        sock.sendall(Preamble().pack() + Message(Op.HELLO, hello).pack_head())
        _recv_exactly(sock, Preamble.SIZE)
        challenge = _recv_message(sock)
        proof = server.token.proof(
            'client',
            client_nonce,
            bytes.fromhex(decode_meta(challenge.meta)['nonce']),
        )
        sock.sendall(
            Message(Op.AUTH, encode_meta({'proof': proof.hex()})).pack_head()
        )
        assert _recv_message(sock).code == Status.OK


async def test_client_version_mismatch_logged_once(
    server: _Server,
    caplog,
) -> None:
    def _warnings() -> list[str]:
        return [
            r.message
            for r in caplog.records
            if 'uses different versions' in r.message
        ]

    await asyncio.to_thread(_raw_handshake, server, Versions.current())
    assert len(_warnings()) == 0

    versions = Versions(proxystore='0.0.1', python='2.7.18')
    await asyncio.to_thread(_raw_handshake, server, versions)
    await asyncio.to_thread(_raw_handshake, server, versions)
    assert len(_warnings()) == 1
    assert 'ProxyStore 0.0.1 (client)' in _warnings()[0]
    assert 'Python 2.7.18 (client)' in _warnings()[0]


class _TLSServer(NamedTuple):
    server: _Server
    fingerprint: str


@pytest_asyncio.fixture()
async def tls_server(
    tmp_path: pathlib.Path,
) -> AsyncGenerator[_TLSServer, None]:
    certificate = TLSCertificate.generate('test')
    context = certificate.ssl_context()

    dispatcher = Dispatcher(EndpointId.random(), MemoryStorage())
    token = EndpointToken.generate()
    handler = ClientHandler(
        dispatcher,
        token,
        name='my-endpoint',
        handshake_timeout=1,
    )
    port = await handler.start('127.0.0.1', 0, ssl_context=context)
    server = _Server(handler, dispatcher, token, '127.0.0.1', port)
    yield _TLSServer(server, certificate.fingerprint)
    await handler.close()
    await dispatcher.storage.close()


def _connect_tls(server: _Server, fingerprint: str) -> EndpointClient:
    return EndpointClient.connect(
        server.host,
        server.port,
        server.token,
        tls_fingerprint=fingerprint,
    )


async def test_tls_operations(tls_server: _TLSServer) -> None:
    client = await asyncio.to_thread(
        _connect_tls,
        tls_server.server,
        tls_server.fingerprint,
    )
    assert isinstance(client._socket, ssl.SSLSocket)

    data = randbytes(5_000_000)
    await asyncio.to_thread(client.set, 'key', data)
    assert await asyncio.to_thread(client.get, 'key') == data
    assert await asyncio.to_thread(client.exists, 'key')
    await asyncio.to_thread(client.close)


async def test_tls_wrong_fingerprint(tls_server: _TLSServer) -> None:
    with pytest.raises(EndpointAuthError, match='TLS certificate'):
        await asyncio.to_thread(_connect_tls, tls_server.server, '0' * 64)


async def test_tls_client_without_tls(tls_server: _TLSServer) -> None:
    with pytest.raises(EndpointConnectionError):
        await _connect(tls_server.server)


async def test_tls_client_with_plain_server(server: _Server) -> None:
    with pytest.raises(EndpointProtocolError, match='TLS handshake'):
        await asyncio.to_thread(_connect_tls, server, '0' * 64)


async def test_ping(server: _Server) -> None:
    client = await _connect(server)
    assert await asyncio.to_thread(client.ping) == PingResult()
    await asyncio.to_thread(client.close)


@pytest.mark.parametrize(
    ('peername', 'expected'),
    (
        (('127.0.0.1', 5000), '127.0.0.1:5000'),
        (('::1', 5000, 0, 0), '[::1]:5000'),
        ('/tmp/socket', '/tmp/socket'),
        (None, 'None'),
    ),
)
def test_format_address(peername: Any, expected: str) -> None:
    assert _format_address(peername) == expected


async def test_client_connection_is_logged(server: _Server, caplog) -> None:
    caplog.set_level(logging.INFO, logger='proxystore.endpoint.server')
    client = await _connect(server)
    await asyncio.to_thread(client.close)
    # Wait for the server to handle the client closing the connection
    await wait_until(
        lambda: any('closed' in r.message for r in caplog.records),
    )
    messages = [r.message for r in caplog.records]
    assert any(
        'Accepted connection from client 127.0.0.1:' in m for m in messages
    )
    assert any('Connection with client 127.0.0.1:' in m for m in messages)
