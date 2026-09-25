from __future__ import annotations

import asyncio
import multiprocessing
import os
import pathlib
import socket
import stat
import uuid
from collections.abc import AsyncGenerator
from typing import Any
from typing import NamedTuple
from unittest import mock
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from globus_sdk.token_storage import TokenValidationError

from proxystore.endpoint.auth import compute_proof
from proxystore.endpoint.auth import read_token_file
from proxystore.endpoint.auth import TOKEN_SIZE
from proxystore.endpoint.client import _recv_exactly
from proxystore.endpoint.client import _recv_message
from proxystore.endpoint.client import EndpointClient
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import EndpointStorageConfig
from proxystore.endpoint.config import get_token_filepath
from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.exceptions import EndpointAuthError
from proxystore.endpoint.exceptions import EndpointClientError
from proxystore.endpoint.exceptions import EndpointRequestError
from proxystore.endpoint.exceptions import PeerRequestError
from proxystore.endpoint.protocol import HEADER
from proxystore.endpoint.protocol import local_versions
from proxystore.endpoint.protocol import MAX_META_SIZE
from proxystore.endpoint.protocol import Op
from proxystore.endpoint.protocol import pack_message
from proxystore.endpoint.protocol import pack_preamble
from proxystore.endpoint.protocol import PREAMBLE
from proxystore.endpoint.protocol import PROTOCOL_VERSION
from proxystore.endpoint.protocol import Status
from proxystore.endpoint.protocol import unpack_preamble
from proxystore.endpoint.serve import _get_auth_headers
from proxystore.endpoint.serve import _serve_async
from proxystore.endpoint.serve import ClientConnection
from proxystore.endpoint.serve import EndpointServer
from proxystore.endpoint.serve import serve
from proxystore.endpoint.storage import DictStorage
from testing.compat import randbytes
from testing.endpoint import terminate_process
from testing.endpoint import wait_for_endpoint
from testing.mocked.globus import get_testing_app
from testing.utils import open_port

MAX_OBJECT_SIZE = 10_000_000


class _Server(NamedTuple):
    handler: EndpointServer
    endpoint: Endpoint
    token: bytes
    host: str
    port: int


@pytest_asyncio.fixture()
async def server() -> AsyncGenerator[_Server, None]:
    async with Endpoint(name='my-endpoint', uuid=uuid.uuid4()) as endpoint:
        token = os.urandom(TOKEN_SIZE)
        handler = EndpointServer(
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
    assert client.info.proxystore_version == local_versions()['proxystore']
    assert client.info.python_version == local_versions()['python']

    small, large = b'value', randbytes(5_000_000)
    await asyncio.to_thread(client.set, 'small', small)
    await asyncio.to_thread(client.set, 'large', large)
    assert await asyncio.to_thread(client.get, 'small') == small
    assert await asyncio.to_thread(client.get, 'large') == large
    assert await asyncio.to_thread(client.exists, 'small')

    await asyncio.to_thread(client.evict, 'small')
    assert not await asyncio.to_thread(client.exists, 'small')
    assert await asyncio.to_thread(client.get, 'small') is None

    await asyncio.to_thread(client.close)


async def test_client_closes_between_requests(server: _Server) -> None:
    client = await _connect(server)
    await asyncio.to_thread(client.exists, 'key')
    await asyncio.to_thread(client.close)
    # Wait for the server to notice the closed connection
    for _ in range(100):  # pragma: no branch
        if len(server.handler._connections) == 0:
            break
        await asyncio.sleep(0.01)
    assert len(server.handler._connections) == 0


async def test_client_wrong_token(server: _Server) -> None:
    # The client detects the server does not know the client's token first
    # (i.e., an impostor endpoint) before sending its own proof.
    with pytest.raises(EndpointAuthError, match='failed to prove'):
        await _connect(server, token=os.urandom(TOKEN_SIZE))


def _raw_hello(sock: socket.socket) -> tuple[bytes, dict[str, Any]]:
    client_nonce = os.urandom(32)
    hello = {'nonce': client_nonce.hex(), **local_versions()}
    sock.sendall(pack_preamble() + pack_message(Op.HELLO, hello))
    assert unpack_preamble(bytes(_recv_exactly(sock, PREAMBLE.size))) == 1
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
            sock.sendall(pack_preamble(PROTOCOL_VERSION + 1))
            preamble = _recv_exactly(sock, PREAMBLE.size)
            assert unpack_preamble(bytes(preamble)) == PROTOCOL_VERSION
            header, meta = _recv_message(sock)
            assert header.code == Status.PROTOCOL_MISMATCH
            assert 'protocol version' in meta['error']
            assert _is_closed(sock)

    await asyncio.to_thread(_run)


@pytest.mark.parametrize(
    'message',
    (
        # Bad magic
        b'XXXX\x00\x01',
        # First message is not HELLO
        pack_preamble() + pack_message(Op.AUTH, {'proof': '00'}),
        # HELLO is missing the nonce
        pack_preamble() + pack_message(Op.HELLO, {}),
        # HELLO has malformed metadata
        pack_preamble() + HEADER.pack(Op.HELLO, 0, 2, 0) + b'[]',
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
    assert 'missing key' in meta['error']

    code, meta = await _raw_request(client, pack_message(99, {'key': 'key'}))
    assert code == Status.BAD_REQUEST
    assert 'unknown op' in meta['error']

    code, meta = await _raw_request(
        client,
        pack_message(Op.GET, {'key': 'key', 'endpoint': 42}),
    )
    assert code == Status.BAD_REQUEST
    assert 'not a valid UUID4' in meta['error']

    with pytest.raises(EndpointRequestError, match='not a valid UUID4'):
        await asyncio.to_thread(client.get, 'key', 'not-a-uuid')

    with pytest.raises(EndpointRequestError, match='empty payload'):
        await asyncio.to_thread(client.set, 'key', b'')

    # The connection is still usable after these errors
    assert not await asyncio.to_thread(client.exists, 'key')
    await asyncio.to_thread(client.close)


async def test_request_meta_too_large(server: _Server) -> None:
    client = await _connect(server)

    def _run() -> None:
        header = HEADER.pack(Op.GET, 0, MAX_META_SIZE + 1, 0)
        client._socket.sendall(header)
        assert _is_closed(client._socket)

    await asyncio.to_thread(_run)
    await asyncio.to_thread(client.close)


async def test_data_too_large(server: _Server) -> None:
    client = await _connect(server)
    assert client.info.max_object_size == MAX_OBJECT_SIZE

    # The client checks the size before sending the data
    data = randbytes(MAX_OBJECT_SIZE + 1)
    with pytest.raises(EndpointRequestError, match='exceeds the maximum'):
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
    with pytest.raises(EndpointRequestError, match='TOO_LARGE'):
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
    with pytest.raises(EndpointClientError):
        await asyncio.to_thread(client.exists, 'key')
    assert client.closed


def _endpoint_config(**kwargs: Any) -> EndpointConfig:
    options: dict[str, Any] = {
        'name': 'my-endpoint',
        'uuid': str(uuid.uuid4()),
        'host': '127.0.0.1',
        'port': open_port(),
        'storage': EndpointStorageConfig(database_path=':memory:'),
    }
    options.update(kwargs)
    return EndpointConfig(**options)


async def test_serve_async_token_file(tmp_path: pathlib.Path) -> None:
    config = _endpoint_config()
    token_file = get_token_filepath(str(tmp_path))
    stop = asyncio.Event()
    task = asyncio.create_task(_serve_async(config, str(tmp_path), stop))

    for _ in range(500):  # pragma: no branch
        if os.path.exists(token_file):
            break
        await asyncio.sleep(0.01)
    assert stat.S_IMODE(os.stat(token_file).st_mode) == 0o600

    assert config.host is not None
    await asyncio.to_thread(wait_for_endpoint, config.host, config.port)
    token = read_token_file(token_file)
    client = await asyncio.to_thread(
        EndpointClient.connect,
        config.host,
        config.port,
        token,
    )
    assert client.info.uuid == uuid.UUID(config.uuid)

    stop.set()
    await task
    # Open connections are closed and the token is removed on shutdown
    with pytest.raises(EndpointClientError):
        await asyncio.to_thread(client.exists, 'key')
    assert not os.path.exists(token_file)


async def test_serve_async_port_in_use(tmp_path: pathlib.Path) -> None:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        sock.listen()
        config = _endpoint_config(port=sock.getsockname()[1])
        with pytest.raises(OSError):
            await _serve_async(config, str(tmp_path))
    assert not os.path.exists(get_token_filepath(str(tmp_path)))


@pytest.mark.timeout(10)
def test_serve(use_uvloop: bool, tmp_path: pathlib.Path) -> None:
    config = _endpoint_config()
    endpoint_dir = str(tmp_path)

    context = multiprocessing.get_context('spawn')
    process = context.Process(
        target=serve,
        args=(config,),
        kwargs={'endpoint_dir': endpoint_dir, 'use_uvloop': use_uvloop},
    )
    process.start()

    try:
        assert config.host is not None
        wait_for_endpoint(config.host, config.port)
        token = read_token_file(get_token_filepath(endpoint_dir))
        with EndpointClient.connect(config.host, config.port, token) as client:
            client.set('key', b'value')
            assert client.get('key') == b'value'

        # SIGTERM should cleanly shutdown the endpoint
        process.terminate()
        process.join(timeout=5)
        assert process.exitcode == 0
        assert not os.path.exists(get_token_filepath(endpoint_dir))
    finally:
        terminate_process(process)


def test_serve_config_validation(
    use_uvloop: bool,
    tmp_path: pathlib.Path,
) -> None:
    config = _endpoint_config(host=None)
    with pytest.raises(ValueError, match='host'):
        serve(config, endpoint_dir=str(tmp_path), use_uvloop=use_uvloop)


def test_serve_logging(use_uvloop: bool, tmp_path: pathlib.Path) -> None:
    # Make a sub dir that should not exist to check serve makes the dir
    tmp_dir = os.path.join(tmp_path, 'log-dir')

    def _serve(log_file: str) -> None:
        with mock.patch(
            'proxystore.endpoint.serve._serve_async',
            AsyncMock(),
        ):
            serve(
                _endpoint_config(),
                endpoint_dir=str(tmp_path),
                log_level='INFO',
                log_file=log_file,
                use_uvloop=use_uvloop,
            )

    # Make directory if necessary
    log_file = os.path.join(tmp_dir, 'log.txt')
    _serve(log_file)
    assert os.path.isdir(tmp_dir)
    assert os.path.exists(log_file)

    # Write log to existing log directory
    log_file2 = os.path.join(tmp_dir, 'log2.txt')
    _serve(log_file2)
    assert os.path.isdir(tmp_dir)
    assert os.path.exists(log_file2)


def test_get_auth_headers_none() -> None:
    assert _get_auth_headers(None) == {}


def test_get_auth_headers_globus() -> None:
    globus_app = get_testing_app()
    mock_authorizer = mock.MagicMock()
    header = 'Bearer <TOKEN>'

    with (
        mock.patch(
            'proxystore.endpoint.serve.get_globus_app',
            return_value=globus_app,
        ),
        mock.patch.object(
            globus_app,
            'get_authorizer',
            return_value=mock_authorizer,
        ),
        mock.patch.object(
            mock_authorizer,
            'get_authorization_header',
            return_value=header,
        ),
    ):
        assert _get_auth_headers('globus')['Authorization'] == header


def test_get_auth_headers_globus_missing() -> None:
    globus_app = get_testing_app()

    with (
        mock.patch(
            'proxystore.endpoint.serve.get_globus_app',
            return_value=globus_app,
        ),
        mock.patch.object(
            globus_app,
            'get_authorizer',
            side_effect=TokenValidationError(),
        ),
        pytest.raises(
            SystemExit,
        ),
    ):
        assert _get_auth_headers('globus')


async def test_serve_cancels_nat_check(
    relay_server,
    tmp_path: pathlib.Path,
) -> None:
    # The NAT check runs concurrently with serving so that a slow or blocked
    # network cannot delay the endpoint from accepting requests. Shutting the
    # endpoint down must therefore cancel a check which has not finished
    # rather than wait for it.
    cancelled = asyncio.Event()

    async def never_finishes() -> None:
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    config = _endpoint_config()
    config.relay.address = relay_server.address

    stop = asyncio.Event()
    stop.set()
    with mock.patch(
        'proxystore.endpoint.serve.check_nat_and_log',
        side_effect=never_finishes,
    ):
        await _serve_async(config, str(tmp_path), stop)

    assert cancelled.is_set()


class _FakeTransport:
    def __init__(self) -> None:
        self.protocol: ClientConnection | None = None
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


async def _fake_connection() -> tuple[ClientConnection, _FakeTransport]:
    conn = ClientConnection(AsyncMock())
    transport = _FakeTransport()
    transport.protocol = conn
    conn.connection_made(transport)  # type: ignore[arg-type]
    await asyncio.sleep(0)
    return conn, transport


def _feed(conn: ClientConnection, data: bytes) -> None:
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
    data = randbytes(ClientConnection._MAX_PENDING_SIZE + 1)
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
