from __future__ import annotations

import asyncio
import os
import pathlib
import socket
import ssl
import uuid
from collections.abc import AsyncGenerator
from typing import Any
from typing import NamedTuple
from unittest import mock
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from proxystore.endpoint.auth import compute_proof
from proxystore.endpoint.auth import create_server_ssl_context
from proxystore.endpoint.auth import Credentials
from proxystore.endpoint.auth import TOKEN_SIZE
from proxystore.endpoint.client import _recv_exactly
from proxystore.endpoint.client import _recv_message
from proxystore.endpoint.client import EndpointClient
from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.exceptions import EndpointAuthError
from proxystore.endpoint.exceptions import EndpointConnectionError
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.exceptions import EndpointRequestError
from proxystore.endpoint.exceptions import ObjectSizeExceededError
from proxystore.endpoint.exceptions import PeerRequestError
from proxystore.endpoint.protocol import Header
from proxystore.endpoint.protocol import Hello
from proxystore.endpoint.protocol import MAX_META_SIZE
from proxystore.endpoint.protocol import Op
from proxystore.endpoint.protocol import pack_message
from proxystore.endpoint.protocol import Preamble
from proxystore.endpoint.protocol import PROTOCOL_VERSION
from proxystore.endpoint.protocol import Request
from proxystore.endpoint.protocol import Status
from proxystore.endpoint.protocol import Versions
from proxystore.endpoint.server import _ClientConnection
from proxystore.endpoint.server import ClientHandler
from proxystore.endpoint.storage import DictStorage
from testing.compat import randbytes

MAX_OBJECT_SIZE = 10_000_000


class _Server(NamedTuple):
    handler: ClientHandler
    endpoint: Endpoint
    token: bytes
    host: str
    port: int


@pytest_asyncio.fixture()
async def server() -> AsyncGenerator[_Server, None]:
    async with Endpoint(name='my-endpoint', uuid=uuid.uuid4()) as endpoint:
        token = os.urandom(TOKEN_SIZE)
        handler = ClientHandler(
            endpoint,
            token,
            max_object_size=MAX_OBJECT_SIZE,
            handshake_timeout=1,
        )
        tcp_server = await handler.start_server('127.0.0.1', 0)
        port = tcp_server.sockets[0].getsockname()[1]
        yield _Server(handler, endpoint, token, '127.0.0.1', port)
        tcp_server.close()
        handler.close_connections()
        await tcp_server.wait_closed()


async def _connect(server: _Server, token: bytes | None = None) -> Any:
    token = server.token if token is None else token
    return await asyncio.to_thread(
        EndpointClient.connect,
        server.host,
        server.port,
        token,
    )


def _raw_socket(server: _Server) -> socket.socket:
    sock = socket.create_connection((server.host, server.port), timeout=5)
    return sock


def _is_closed(sock: socket.socket) -> bool:
    try:
        return sock.recv(1) == b''
    except ConnectionResetError:  # pragma: no cover
        return True


async def test_operations(server: _Server) -> None:
    client = await _connect(server)
    assert client.info.uuid == server.endpoint.uuid
    assert client.info.name == server.endpoint.name
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
        await _connect(server, token=os.urandom(TOKEN_SIZE))


def _raw_hello(sock: socket.socket) -> tuple[bytes, dict[str, Any]]:
    client_nonce = os.urandom(32)
    hello = Hello(client_nonce, Versions.current()).to_meta()
    sock.sendall(Preamble().pack() + pack_message(Op.HELLO, hello))
    assert (
        Preamble.unpack(bytes(_recv_exactly(sock, Preamble.SIZE))).version == 1
    )
    header, meta = _recv_message(sock)
    assert header.code == Status.OK
    return client_nonce, meta


async def test_server_rejects_bad_proof(server: _Server) -> None:
    def _run() -> None:
        with _raw_socket(server) as sock:
            client_nonce, meta = _raw_hello(sock)
            proof = compute_proof(
                os.urandom(TOKEN_SIZE),
                'client',
                client_nonce,
                bytes.fromhex(meta['nonce']),
            )
            sock.sendall(pack_message(Op.AUTH, {'proof': proof.hex()}))
            header, meta = _recv_message(sock)
            assert header.code == Status.UNAUTHORIZED
            assert _is_closed(sock)

    await asyncio.to_thread(_run)


async def test_server_rejects_replayed_server_proof(server: _Server) -> None:
    # The server's own proof cannot be sent back as the client's proof.
    def _run() -> None:
        with _raw_socket(server) as sock:
            _, meta = _raw_hello(sock)
            sock.sendall(pack_message(Op.AUTH, {'proof': meta['proof']}))
            header, _ = _recv_message(sock)
            assert header.code == Status.UNAUTHORIZED

    await asyncio.to_thread(_run)


async def test_protocol_version_mismatch(server: _Server) -> None:
    def _run() -> None:
        with _raw_socket(server) as sock:
            # The HELLO of a different protocol version is never read
            hello = pack_message(Op.HELLO, {'future': 'format'})
            sock.sendall(Preamble(PROTOCOL_VERSION + 1).pack() + hello)
            preamble = _recv_exactly(sock, Preamble.SIZE)
            assert Preamble.unpack(bytes(preamble)).version == PROTOCOL_VERSION
            assert _is_closed(sock)

    await asyncio.to_thread(_run)


@pytest.mark.parametrize(
    'message',
    (
        # Bad magic
        b'XXXX\x00\x01',
        # First message is not HELLO
        Preamble().pack() + pack_message(Op.AUTH, {'proof': '00'}),
        # HELLO is missing the nonce
        Preamble().pack() + pack_message(Op.HELLO, {}),
        # HELLO has malformed metadata
        Preamble().pack() + Header(Op.HELLO, 0, 2, 0).pack() + b'[]',
        # HELLO nonce is too short
        Preamble().pack()
        + pack_message(
            Op.HELLO,
            Hello(os.urandom(16), Versions.current()).to_meta(),
        ),
        # HELLO contains data
        Preamble().pack()
        + pack_message(
            Op.HELLO,
            Hello(os.urandom(32), Versions.current()).to_meta(),
            data_len=1,
        )
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
            sock.sendall(pack_message(Op.GET, {'key': 'key'}))
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
        header, meta = _recv_message(client._socket)
        return header.code, meta

    return await asyncio.to_thread(_run)


async def test_bad_requests(server: _Server) -> None:
    client = await _connect(server)

    code, meta = await _raw_request(client, pack_message(Op.GET, {}))
    assert code == Status.BAD_REQUEST
    assert "invalid 'key'" in meta['error']

    request = Request('key').to_meta()
    code, meta = await _raw_request(client, pack_message(99, request))
    assert code == Status.BAD_REQUEST
    assert 'unknown op' in meta['error']

    code, meta = await _raw_request(
        client,
        pack_message(Op.GET, {'key': 'key', 'endpoint': 'not-a-uuid'}),
    )
    assert code == Status.BAD_REQUEST
    assert "invalid 'endpoint'" in meta['error']

    # The client validates the endpoint UUID before sending the request
    with pytest.raises(ValueError, match='not a valid endpoint UUID'):
        await asyncio.to_thread(client.get, 'key', 'not-a-uuid')

    with pytest.raises(EndpointRequestError, match='empty payload'):
        await asyncio.to_thread(client.set, 'key', b'')

    # The connection is still usable after these errors
    assert not await asyncio.to_thread(client.exists, 'key')
    await asyncio.to_thread(client.close)


async def test_request_meta_too_large(server: _Server) -> None:
    client = await _connect(server)

    def _run() -> None:
        header = Header(Op.GET, 0, MAX_META_SIZE + 1, 0).pack()
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

    # The server also checks the size before reading the data, then closes
    # the connection because it did not read the data
    code, meta = await _raw_request(
        client,
        pack_message(Op.SET, {'key': 'key'}, data_len=MAX_OBJECT_SIZE + 1),
    )
    assert code == Status.TOO_LARGE
    assert 'exceeds the maximum' in meta['error']
    await asyncio.to_thread(lambda: _is_closed(client._socket))
    await asyncio.to_thread(client.close)


async def test_storage_object_size_exceeded(server: _Server) -> None:
    server.endpoint._storage = DictStorage(max_object_size=10)
    client = await _connect(server)
    with pytest.raises(ObjectSizeExceededError, match='TOO_LARGE'):
        await asyncio.to_thread(client.set, 'key', randbytes(100))
    await asyncio.to_thread(client.close)


async def test_peer_request_error(server: _Server) -> None:
    client = await _connect(server)
    with mock.patch.object(
        server.endpoint,
        'get',
        AsyncMock(side_effect=PeerRequestError('peer failed')),
    ):
        with pytest.raises(EndpointRequestError, match='peer failed'):
            await asyncio.to_thread(client.get, 'key', str(uuid.uuid4()))
    await asyncio.to_thread(client.close)


async def test_unexpected_error(server: _Server) -> None:
    client = await _connect(server)
    with mock.patch.object(
        server.endpoint,
        'exists',
        AsyncMock(side_effect=RuntimeError('oops')),
    ):
        with pytest.raises(EndpointRequestError, match='unexpected error'):
            await asyncio.to_thread(client.exists, 'key')
    await asyncio.to_thread(client.close)


async def test_close_connections(server: _Server) -> None:
    client = await _connect(server)
    server.handler.close_connections()
    with pytest.raises(EndpointConnectionError):
        await asyncio.to_thread(client.exists, 'key')
    assert client.closed


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
    conn = _ClientConnection(AsyncMock())
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
        hello = Hello(client_nonce, versions).to_meta()
        sock.sendall(Preamble().pack() + pack_message(Op.HELLO, hello))
        _recv_exactly(sock, Preamble.SIZE)
        _, meta = _recv_message(sock)
        proof = compute_proof(
            server.token,
            'client',
            client_nonce,
            bytes.fromhex(meta['nonce']),
        )
        sock.sendall(pack_message(Op.AUTH, {'proof': proof.hex()}))
        header, _ = _recv_message(sock)
        assert header.code == Status.OK


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
    credentials = Credentials.create(
        str(tmp_path),
        tls=True,
        common_name='test',
    )
    assert credentials.tls_fingerprint is not None
    context = create_server_ssl_context(str(tmp_path))

    async with Endpoint(name='my-endpoint', uuid=uuid.uuid4()) as endpoint:
        token = credentials.token
        handler = ClientHandler(endpoint, token, handshake_timeout=1)
        tcp_server = await handler.start_server(
            '127.0.0.1',
            0,
            ssl_context=context,
        )
        port = tcp_server.sockets[0].getsockname()[1]
        server = _Server(handler, endpoint, token, '127.0.0.1', port)
        yield _TLSServer(server, credentials.tls_fingerprint)
        tcp_server.close()
        handler.close_connections()
        await tcp_server.wait_closed()


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
