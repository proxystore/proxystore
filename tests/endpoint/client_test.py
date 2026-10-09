from __future__ import annotations

import contextlib
import os
import pathlib
import socket
import struct
import threading
import time
import warnings
from collections.abc import Callable
from collections.abc import Generator
from typing import Any
from unittest import mock

import pytest

from proxystore.endpoint import protocol
from proxystore.endpoint.auth import EndpointToken
from proxystore.endpoint.auth import TLSCertificate
from proxystore.endpoint.client import _enable_keepalive
from proxystore.endpoint.client import _recv_exactly
from proxystore.endpoint.client import _recv_message
from proxystore.endpoint.client import _send_all
from proxystore.endpoint.client import _wrap_tls
from proxystore.endpoint.client import EndpointClient
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.directory import ConnectionInfo
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.exceptions import EndpointAuthError
from proxystore.endpoint.exceptions import EndpointConnectionError
from proxystore.endpoint.exceptions import EndpointNotFoundError
from proxystore.endpoint.exceptions import EndpointNotRunningError
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.exceptions import EndpointRequestError
from proxystore.endpoint.exceptions import EndpointTimeoutError
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.protocol import EndpointInfo
from proxystore.endpoint.protocol import Message
from proxystore.endpoint.protocol import Preamble
from proxystore.endpoint.protocol import PROTOCOL_VERSION
from proxystore.endpoint.protocol import Status
from proxystore.endpoint.protocol import Versions
from proxystore.warnings import VersionMismatchWarning
from testing.endpoint import decode_meta
from testing.endpoint import encode_meta

TOKEN = EndpointToken.generate()
ENDPOINT_ID = EndpointId.random()

Script = Callable[[socket.socket], None]


def _info(**overrides: Any) -> dict[str, Any]:
    info = EndpointInfo(
        id=ENDPOINT_ID,
        name='fake',
        versions=Versions.current(),
        max_object_size=None,
    ).model_dump(mode='json')
    info.update(overrides)
    return info


def _versions(**overrides: str) -> dict[str, str]:
    return {**Versions.current().model_dump(), **overrides}


def _server_hello(
    conn: socket.socket,
    *,
    version: int = PROTOCOL_VERSION,
    status: int = Status.OK,
    meta: dict[str, Any] | None = None,
) -> bytes:
    """Read the client's HELLO and reply. Returns the client nonce."""
    _recv_exactly(conn, Preamble.SIZE)
    hello = _recv_message(conn)
    client_nonce = bytes.fromhex(decode_meta(hello.meta)['nonce'])
    if meta is None:
        server_nonce = os.urandom(32)
        proof = TOKEN.proof('server', server_nonce, client_nonce)
        meta = {'nonce': server_nonce.hex(), 'proof': proof.hex()}
    conn.sendall(
        Preamble(version).pack()
        + Message(status, encode_meta(meta)).pack_head()
    )
    return client_nonce


def _complete_handshake(conn: socket.socket) -> None:
    _server_hello(conn)
    _recv_message(conn)
    conn.sendall(Message(Status.OK, encode_meta(_info())).pack_head())


@pytest.fixture
def fake_server() -> Generator[Callable[[Script], int], None, None]:
    """Fake endpoint that runs a script on the first connection."""
    listener = socket.create_server(('127.0.0.1', 0))
    threads: list[threading.Thread] = []

    def _start(script: Script) -> int:
        def _run() -> None:
            conn, _ = listener.accept()
            with conn, contextlib.suppress(OSError):
                script(conn)

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
    with pytest.raises(EndpointNotRunningError, match='refused'):
        EndpointClient.connect('127.0.0.1', port, TOKEN, timeout=1)


def test_connect_unreachable() -> None:
    with (
        mock.patch('socket.create_connection', side_effect=TimeoutError),
        pytest.raises(EndpointConnectionError, match='Unable to connect'),
    ):
        EndpointClient.connect('127.0.0.1', 1, TOKEN, timeout=1)


def test_connect_handshake_timeout(fake_server) -> None:
    done = threading.Event()
    port = fake_server(lambda conn: done.wait(timeout=5))
    try:
        with pytest.raises(EndpointConnectionError, match='handshake'):
            EndpointClient.connect('127.0.0.1', port, TOKEN, timeout=0.1)
    finally:
        done.set()


def test_connect_and_close(fake_server) -> None:
    port = fake_server(_complete_handshake)
    with EndpointClient.connect('127.0.0.1', port, TOKEN) as client:
        assert client.info.id == ENDPOINT_ID
        assert client.info.name == 'fake'
        assert client.info.max_object_size is None
        assert client.protocol_version == PROTOCOL_VERSION
        assert 'fake' in repr(client)
    assert client.closed
    # Closing again is a no-op
    client.close()


def test_connect_older_protocol_version(
    fake_server,
) -> None:
    # A client which also supports a newer version uses the older version
    # negotiated by the endpoint.
    port = fake_server(_complete_handshake)
    with (
        mock.patch.object(protocol, 'PROTOCOL_VERSION', PROTOCOL_VERSION + 1),
        EndpointClient.connect('127.0.0.1', port, TOKEN) as client,
    ):
        assert client.protocol_version == PROTOCOL_VERSION

    with pytest.raises(EndpointConnectionError, match='closed'):
        client.exists('key')


def test_handshake_protocol_mismatch(fake_server) -> None:
    def _script(conn: socket.socket) -> None:
        _recv_exactly(conn, Preamble.SIZE)
        # Nothing after the preamble of a different protocol version is
        # parsed, so it may be in any format.
        conn.sendall(Preamble(PROTOCOL_VERSION + 1).pack() + b'\xff' * 64)

    port = fake_server(_script)
    with pytest.raises(EndpointProtocolError, match='protocol version'):
        EndpointClient.connect('127.0.0.1', port, TOKEN)


def test_handshake_message_with_data(fake_server) -> None:
    def _script(conn: socket.socket) -> None:
        _recv_exactly(conn, Preamble.SIZE)
        _recv_message(conn)
        conn.sendall(
            Preamble().pack()
            + Message(Status.OK, encode_meta({})).pack_head(1)
            + b'x',
        )

    port = fake_server(_script)
    with pytest.raises(EndpointProtocolError, match='contains data'):
        EndpointClient.connect('127.0.0.1', port, TOKEN)


@pytest.mark.parametrize(
    ('stage', 'status', 'meta', 'error', 'match'),
    (
        # Reply to HELLO
        ('hello', Status.ERROR, {}, EndpointProtocolError, 'no error message'),
        (
            'hello',
            Status.OK,
            {'nonce': 'x'},
            EndpointProtocolError,
            'Malformed',
        ),
        # Reply to AUTH
        (
            'auth',
            Status.UNAUTHORIZED,
            {'error': 'no'},
            EndpointAuthError,
            'rejected',
        ),
        ('auth', Status.ERROR, {'error': 'bad'}, EndpointProtocolError, 'bad'),
        (
            'auth',
            Status.OK,
            _info(id='not-an-id'),
            EndpointProtocolError,
            'Malformed',
        ),
        ('auth', Status.OK, {}, EndpointProtocolError, 'Malformed'),
    ),
)
def test_handshake_bad_reply(
    stage: str,
    status: int,
    meta: dict[str, Any],
    error: type[Exception],
    match: str,
    fake_server,
) -> None:
    def _script(conn: socket.socket) -> None:
        if stage == 'hello':
            _server_hello(conn, status=status, meta=meta)
            return
        _server_hello(conn)
        _recv_message(conn)
        conn.sendall(Message(status, encode_meta(meta)).pack_head())

    port = fake_server(_script)
    with pytest.raises(error, match=match):
        EndpointClient.connect('127.0.0.1', port, TOKEN)


def _respond_with(
    status: int,
    meta: dict[str, Any] | None = None,
    *,
    request_id: int | None = None,
) -> Script:
    def _script(conn: socket.socket) -> None:
        _complete_handshake(conn)
        request = _recv_message(conn)
        request_id_ = request.request_id if request_id is None else request_id
        response = Message(
            status, encode_meta(meta or {}), request_id=request_id_
        )
        conn.sendall(response.pack_head())

    return _script


def test_request_ids(fake_server) -> None:
    ids: list[int] = []

    def _script(conn: socket.socket) -> None:
        _complete_handshake(conn)
        for _ in range(3):
            request = _recv_message(conn)
            ids.append(request.request_id)
            response = Message(
                Status.OK,
                encode_meta({'exists': True}),
                request_id=request.request_id,
            ).pack_head()
            conn.sendall(response)

    port = fake_server(_script)
    with EndpointClient.connect('127.0.0.1', port, TOKEN) as client:
        client._next_request_id = 2**32 - 2
        for _ in range(3):
            assert client.exists('key')
    # IDs wrap around to 1 because 0 is reserved for handshake messages
    assert ids == [2**32 - 2, 2**32 - 1, 1]


@pytest.mark.parametrize(
    ('status', 'meta', 'request_id', 'error', 'match', 'closed'),
    (
        # The connection is unusable after a response to another request
        (Status.OK, None, 42, EndpointProtocolError, 'request 42', True),
        # The connection is in an unknown state after a bad request
        (
            Status.BAD_REQUEST,
            {'error': 'bad'},
            None,
            EndpointProtocolError,
            'BAD_REQUEST .*: bad',
            True,
        ),
        (
            Status.ERROR,
            None,
            None,
            EndpointRequestError,
            'no error message',
            False,
        ),
        # A newer endpoint may return an error status unknown to this client
        (
            99,
            {'error': 'new error'},
            None,
            EndpointRequestError,
            'status code 99.*new error',
            False,
        ),
    ),
)
def test_request_error_response(
    status: int,
    meta: dict[str, Any] | None,
    request_id: int | None,
    error: type[Exception],
    match: str,
    closed: bool,
    fake_server,
) -> None:
    port = fake_server(_respond_with(status, meta, request_id=request_id))
    with EndpointClient.connect('127.0.0.1', port, TOKEN) as client:
        with pytest.raises(error, match=match) as e:
            client.exists('key')
        assert type(e.value) is error
        assert client.closed == closed


def test_request_connection_closed(fake_server) -> None:
    def _script(conn: socket.socket) -> None:
        _complete_handshake(conn)
        _recv_message(conn)

    port = fake_server(_script)
    with EndpointClient.connect('127.0.0.1', port, TOKEN) as client:
        with pytest.raises(
            EndpointConnectionError, match='closed the connection'
        ):
            client.exists('key')
        assert client.closed


def test_request_connection_reset(fake_server) -> None:
    reset = threading.Event()

    def _script(conn: socket.socket) -> None:
        _complete_handshake(conn)
        # Closing with a zero linger timeout resets the connection
        linger = struct.pack('ii', 1, 0)
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, linger)
        conn.close()
        reset.set()

    port = fake_server(_script)
    with EndpointClient.connect('127.0.0.1', port, TOKEN) as client:
        assert reset.wait(timeout=5)
        with pytest.raises(EndpointConnectionError, match='Lost connection'):
            # Large enough that the send cannot complete before the reset
            client.set('key', b'x' * 10_000_000)
        assert client.closed


def test_old_http_endpoint(fake_server) -> None:
    def _script(conn: socket.socket) -> None:
        conn.recv(65536)
        conn.sendall(b'HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n')

    port = fake_server(_script)
    with pytest.raises(EndpointProtocolError, match='responded with HTTP'):
        EndpointClient.connect('127.0.0.1', port, TOKEN)


def _handshake_with_info(**info: Any) -> Script:
    def _script(conn: socket.socket) -> None:
        _server_hello(conn)
        _recv_message(conn)
        conn.sendall(
            Message(Status.OK, encode_meta(_info(**info))).pack_head()
        )

    return _script


def test_version_mismatch_warning(fake_server) -> None:
    # The version depends on the checkout (e.g., 0.1.dev1 without tags), so
    # the different major version is computed from it.
    major = int(Versions.current().proxystore.split('.')[0])
    other = _versions(proxystore=f'{major + 1}.0.0')
    port = fake_server(_handshake_with_info(versions=other))
    with pytest.warns(VersionMismatchWarning, match='ProxyStore'):
        client = EndpointClient.connect('127.0.0.1', port, TOKEN)
    client.close()


def test_version_same_major_no_warning(fake_server) -> None:
    major = Versions.current().proxystore.split('.')[0]
    port = fake_server(
        _handshake_with_info(
            versions=_versions(proxystore=f'{major}.999.0', python='2.7.18'),
        ),
    )
    with warnings.catch_warnings():
        warnings.simplefilter('error', VersionMismatchWarning)
        client = EndpointClient.connect('127.0.0.1', port, TOKEN)
    client.close()


def _write_config(tmp_path: pathlib.Path, **kwargs: Any) -> EndpointDir:
    endpoint_dir = EndpointDir(str(tmp_path))
    config = EndpointConfig(
        name='test',
        id=EndpointId.random(),
        port=1,
        **kwargs,
    )
    endpoint_dir.write_config(config)
    return endpoint_dir


def test_from_dir_missing_directory(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir(str(tmp_path / 'missing'))
    with pytest.raises(EndpointNotFoundError, match='does not exist'):
        EndpointClient.from_dir(endpoint_dir)


def test_from_dir_not_running(tmp_path: pathlib.Path) -> None:
    endpoint_dir = _write_config(tmp_path)
    with pytest.raises(EndpointNotRunningError, match='Is the endpoint'):
        EndpointClient.from_dir(endpoint_dir)


def test_from_dir_running_without_connection_file(
    tmp_path: pathlib.Path,
) -> None:
    # The endpoint holds its lock while it is starting
    endpoint_dir = _write_config(tmp_path)
    lock = endpoint_dir.lock()
    lock.acquire()
    try:
        with pytest.raises(EndpointNotRunningError, match='still starting'):
            EndpointClient.from_dir(endpoint_dir)
    finally:
        lock.release()


@pytest.mark.parametrize(
    ('contents', 'match'),
    ((None, 'Unable to read'), ('not json', 'malformed')),
)
def test_from_dir_bad_connection_file(
    contents: str | None,
    match: str,
    tmp_path: pathlib.Path,
) -> None:
    endpoint_dir = _write_config(tmp_path, host='localhost')
    if contents is None:
        # A directory cannot be read as a file
        os.mkdir(endpoint_dir.connection_path)
    else:
        with open(endpoint_dir.connection_path, 'w') as f:
            f.write(contents)
    with pytest.raises(EndpointAuthError, match=match):
        EndpointClient.from_dir(endpoint_dir)


def test_from_name(tmp_path: pathlib.Path, fake_server) -> None:
    port = fake_server(_complete_handshake)
    endpoint_dir = _write_config(tmp_path / 'test')
    endpoint_dir.write_connection(
        ConnectionInfo(
            host='127.0.0.1',
            port=port,
            token=TOKEN,
            tls_fingerprint=None,
            hostname='machine',
            pid=42,
        ),
    )
    with EndpointClient.from_name('test', proxystore_dir=str(tmp_path)) as c:
        assert c.info.id == ENDPOINT_ID


@pytest.mark.parametrize('method', ('send', 'recv_into'))
def test_request_interrupted_closes_connection(fake_server, method) -> None:
    def _script(conn: socket.socket) -> None:
        _complete_handshake(conn)
        conn.recv(65536)

    port = fake_server(_script)
    client = EndpointClient.connect('127.0.0.1', port, TOKEN)
    sock = mock.MagicMock(wraps=client._socket)
    getattr(sock, method).side_effect = KeyboardInterrupt
    client._socket = sock

    with pytest.raises(KeyboardInterrupt):
        client.exists('key')
    assert client.closed
    sock.close.assert_called_once()


def test_connect_enables_keepalive(fake_server) -> None:
    port = fake_server(_complete_handshake)
    with EndpointClient.connect('127.0.0.1', port, TOKEN) as client:
        sock = client._socket
        assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0
        if hasattr(socket, 'TCP_KEEPIDLE'):  # pragma: no branch
            idle = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE)
            assert idle == 30


@pytest.mark.parametrize('target', (None, ENDPOINT_ID))
def test_request_timeout(fake_server, target: str | None) -> None:
    release = threading.Event()

    def _script(conn: socket.socket) -> None:
        _complete_handshake(conn)
        _recv_message(conn)
        # Never respond.
        release.wait(5)

    port = fake_server(_script)
    client = EndpointClient.connect(
        '127.0.0.1',
        port,
        TOKEN,
        request_timeout=0.1,
    )
    try:
        with pytest.raises(EndpointTimeoutError, match=r'0\.1 seconds'):
            client.exists('key', target=target)
        assert client.closed
    finally:
        release.set()


def test_forwarded_request_has_no_timeout(fake_server) -> None:
    def _script(conn: socket.socket) -> None:
        _complete_handshake(conn)
        request = _recv_message(conn)
        # The endpoint responds once the peer does, which can take longer
        # than the request timeout of the client.
        time.sleep(0.3)
        response = Message(
            Status.OK,
            encode_meta({'exists': True}),
            request_id=request.request_id,
        )
        conn.sendall(response.pack_head())

    port = fake_server(_script)
    with EndpointClient.connect(
        '127.0.0.1',
        port,
        TOKEN,
        request_timeout=0.1,
    ) as client:
        assert client.exists('key', target=EndpointId.random())


def test_send_all_partial_sends() -> None:
    sock = mock.MagicMock()
    sock.send.side_effect = lambda view: min(len(view), 3)
    _send_all(sock, b'abcdefgh')
    sent = [bytes(c.args[0][:3]) for c in sock.send.call_args_list]
    assert sent == [b'abc', b'def', b'gh']


def test_send_all_tls_slow_reader() -> None:
    # The timeout of a TLS socket limits each send() call, so sending must
    # be split for the timeout to only limit the time without progress.
    certificate = TLSCertificate.generate('test')
    context = certificate.ssl_context()
    timeout = 1
    size = 16 * 1024 * 1024
    # The reader reads about 10 MB/s, so each slice is sent well within the
    # timeout (even if sleeps take longer, e.g., on macOS) but sending
    # everything takes longer than the timeout.
    burst = 512 * 1024
    received = 0

    with socket.create_server(('127.0.0.1', 0)) as listener:

        def _read_slowly() -> None:
            nonlocal received
            conn, _ = listener.accept()
            with context.wrap_socket(conn, server_side=True) as tls:
                while received < size:
                    # A TLS recv() returns at most one record (16 KiB).
                    target = min(received + burst, size)
                    while received < target:
                        received += len(tls.recv(burst))
                    time.sleep(0.05)

        thread = threading.Thread(target=_read_slowly, daemon=True)
        thread.start()
        sock = socket.create_connection(listener.getsockname())
        with _wrap_tls(sock, certificate.fingerprint) as tls:
            tls.settimeout(timeout)
            start = time.monotonic()
            _send_all(tls, bytes(size))
            thread.join(timeout=10)
    assert received == size
    # Sending took longer than the timeout, so it was not one deadline.
    assert time.monotonic() - start > timeout


def test_enable_keepalive_ignores_unsupported_options() -> None:
    sock = mock.MagicMock()

    def _setsockopt(level: int, option: int, value: int) -> None:
        if level == socket.IPPROTO_TCP:
            raise OSError('not supported')

    sock.setsockopt.side_effect = _setsockopt
    _enable_keepalive(sock)
    sock.setsockopt.assert_any_call(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)


@pytest.mark.parametrize('timeout', (0, -1))
def test_request_timeout_must_be_positive(timeout: float) -> None:
    with pytest.raises(ValueError, match='must be positive'):
        EndpointClient.connect('127.0.0.1', 1, TOKEN, request_timeout=timeout)
