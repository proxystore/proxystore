from __future__ import annotations

import os
import socket
import threading
import time
import uuid
from collections.abc import Callable
from collections.abc import Generator
from typing import Any

import pytest

from proxystore.endpoint.auth import compute_proof
from proxystore.endpoint.auth import TOKEN_SIZE
from proxystore.endpoint.client import _recv_exactly
from proxystore.endpoint.client import _recv_message
from proxystore.endpoint.client import EndpointClient
from proxystore.endpoint.exceptions import EndpointAuthError
from proxystore.endpoint.exceptions import EndpointClientError
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.exceptions import EndpointRequestError
from proxystore.endpoint.protocol import local_versions
from proxystore.endpoint.protocol import pack_message
from proxystore.endpoint.protocol import pack_preamble
from proxystore.endpoint.protocol import PREAMBLE
from proxystore.endpoint.protocol import PROTOCOL_VERSION
from proxystore.endpoint.protocol import Status

TOKEN = os.urandom(TOKEN_SIZE)
ENDPOINT_UUID = uuid.uuid4()

Script = Callable[[socket.socket], None]


def _info(**overrides: Any) -> dict[str, Any]:
    info = {
        'uuid': str(ENDPOINT_UUID),
        'name': 'fake',
        'max_object_size': None,
        **local_versions(),
    }
    info.update(overrides)
    return info


def _server_hello(
    conn: socket.socket,
    *,
    version: int = PROTOCOL_VERSION,
    status: int = Status.OK,
    meta: dict[str, Any] | None = None,
) -> bytes:
    """Read the client's HELLO and reply. Returns the client nonce."""
    _recv_exactly(conn, PREAMBLE.size)
    _, hello = _recv_message(conn)
    client_nonce = bytes.fromhex(hello['nonce'])
    if meta is None:
        server_nonce = os.urandom(32)
        proof = compute_proof(TOKEN, 'server', server_nonce, client_nonce)
        meta = {'nonce': server_nonce.hex(), 'proof': proof.hex()}
    conn.sendall(pack_preamble(version) + pack_message(status, meta))
    return client_nonce


def _complete_handshake(conn: socket.socket) -> None:
    _server_hello(conn)
    _recv_message(conn)
    conn.sendall(pack_message(Status.OK, _info()))


@pytest.fixture
def fake_server() -> Generator[Callable[[Script], int], None, None]:
    """Fake endpoint that runs a script on the first connection."""
    listener = socket.create_server(('127.0.0.1', 0))
    threads: list[threading.Thread] = []

    def _start(script: Script) -> int:
        def _run() -> None:
            conn, _ = listener.accept()
            with conn:
                try:
                    script(conn)
                except OSError:  # pragma: no cover
                    pass

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        threads.append(thread)
        return listener.getsockname()[1]

    yield _start

    for thread in threads:
        thread.join(timeout=5)
    listener.close()


def test_connect_refused() -> None:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    with pytest.raises(OSError):
        EndpointClient.connect('127.0.0.1', port, TOKEN, timeout=1)


def test_connect_and_close(fake_server) -> None:
    port = fake_server(_complete_handshake)
    with EndpointClient.connect('127.0.0.1', port, TOKEN) as client:
        assert client.info.uuid == ENDPOINT_UUID
        assert client.info.name == 'fake'
        assert client.info.max_object_size is None
        assert 'fake' in repr(client)
    assert client.closed
    # Closing again is a no-op
    client.close()

    with pytest.raises(EndpointClientError, match='closed'):
        client.exists('key')


def test_handshake_protocol_mismatch(fake_server) -> None:
    port = fake_server(
        lambda conn: _server_hello(
            conn,
            version=PROTOCOL_VERSION + 1,
            status=Status.PROTOCOL_MISMATCH,
            meta={'error': 'custom mismatch message'},
        ),
    )
    with pytest.raises(EndpointProtocolError, match='custom mismatch'):
        EndpointClient.connect('127.0.0.1', port, TOKEN)


def test_handshake_protocol_mismatch_no_message(fake_server) -> None:
    port = fake_server(
        lambda conn: _server_hello(conn, version=PROTOCOL_VERSION + 1),
    )
    with pytest.raises(EndpointProtocolError, match='protocol version'):
        EndpointClient.connect('127.0.0.1', port, TOKEN)


def test_handshake_error_status(fake_server) -> None:
    port = fake_server(
        lambda conn: _server_hello(conn, status=Status.ERROR, meta={}),
    )
    with pytest.raises(EndpointProtocolError, match='no error message'):
        EndpointClient.connect('127.0.0.1', port, TOKEN)


def test_handshake_malformed_hello_response(fake_server) -> None:
    port = fake_server(lambda conn: _server_hello(conn, meta={'nonce': 'x'}))
    with pytest.raises(EndpointProtocolError, match='Malformed'):
        EndpointClient.connect('127.0.0.1', port, TOKEN)


def test_handshake_rejected(fake_server) -> None:
    def _script(conn: socket.socket) -> None:
        _server_hello(conn)
        _recv_message(conn)
        conn.sendall(pack_message(Status.UNAUTHORIZED, {'error': 'no'}))

    port = fake_server(_script)
    with pytest.raises(EndpointAuthError, match='rejected'):
        EndpointClient.connect('127.0.0.1', port, TOKEN)


@pytest.mark.parametrize(
    ('status', 'meta', 'match'),
    (
        (Status.ERROR, {'error': 'bad'}, 'bad'),
        (Status.OK, _info(uuid='not-a-uuid'), 'Malformed'),
        (Status.OK, {}, 'Malformed'),
    ),
)
def test_handshake_bad_info(
    status: int,
    meta: dict[str, Any],
    match: str,
    fake_server,
) -> None:
    def _script(conn: socket.socket) -> None:
        _server_hello(conn)
        _recv_message(conn)
        conn.sendall(pack_message(status, meta))

    port = fake_server(_script)
    with pytest.raises(EndpointProtocolError, match=match):
        EndpointClient.connect('127.0.0.1', port, TOKEN)


def _respond_with(status: int, meta: dict[str, Any] | None = None) -> Script:
    def _script(conn: socket.socket) -> None:
        _complete_handshake(conn)
        _recv_message(conn)
        conn.sendall(pack_message(status, meta))

    return _script


def test_request_error_no_message(fake_server) -> None:
    port = fake_server(_respond_with(Status.ERROR))
    with EndpointClient.connect('127.0.0.1', port, TOKEN) as client:
        with pytest.raises(EndpointRequestError, match='no error message'):
            client.exists('key')
        assert not client.closed


def test_request_unknown_status(fake_server) -> None:
    port = fake_server(_respond_with(99))
    with EndpointClient.connect('127.0.0.1', port, TOKEN) as client:
        with pytest.raises(EndpointProtocolError, match='unknown status'):
            client.exists('key')
        assert client.closed


def test_request_connection_closed(fake_server) -> None:
    def _script(conn: socket.socket) -> None:
        _complete_handshake(conn)
        _recv_message(conn)

    port = fake_server(_script)
    with EndpointClient.connect('127.0.0.1', port, TOKEN) as client:
        with pytest.raises(EndpointClientError, match='closed the connection'):
            client.exists('key')
        assert client.closed


def test_request_connection_reset(fake_server) -> None:
    def _script(conn: socket.socket) -> None:
        _complete_handshake(conn)
        conn.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_LINGER,
            b'\x01\x00\x00\x00\x00\x00\x00\x00',
        )

    port = fake_server(_script)
    with EndpointClient.connect('127.0.0.1', port, TOKEN) as client:
        # Wait for the server to reset the connection
        time.sleep(0.2)
        with pytest.raises(EndpointClientError, match='Lost connection'):
            # Large enough that the send cannot complete before the reset
            client.set('key', b'x' * 10_000_000)
        assert client.closed
