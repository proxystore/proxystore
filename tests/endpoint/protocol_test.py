from __future__ import annotations

import json
import os
import platform
from typing import Any

import pytest

import proxystore
from proxystore.endpoint.exceptions import EndpointError
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.exceptions import EndpointRequestError
from proxystore.endpoint.exceptions import ObjectSizeExceededError
from proxystore.endpoint.exceptions import PeerConnectionTimeoutError
from proxystore.endpoint.exceptions import PeerError
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.protocol import ALPN
from proxystore.endpoint.protocol import Auth
from proxystore.endpoint.protocol import Challenge
from proxystore.endpoint.protocol import EndpointInfo
from proxystore.endpoint.protocol import error_status
from proxystore.endpoint.protocol import ErrorInfo
from proxystore.endpoint.protocol import ExistsResult
from proxystore.endpoint.protocol import HandshakeReader
from proxystore.endpoint.protocol import Header
from proxystore.endpoint.protocol import Hello
from proxystore.endpoint.protocol import MAX_META_SIZE
from proxystore.endpoint.protocol import Message
from proxystore.endpoint.protocol import MessageReader
from proxystore.endpoint.protocol import Meta
from proxystore.endpoint.protocol import NONCE_SIZE
from proxystore.endpoint.protocol import Op
from proxystore.endpoint.protocol import PingResult
from proxystore.endpoint.protocol import Preamble
from proxystore.endpoint.protocol import PROTOCOL_VERSION
from proxystore.endpoint.protocol import raise_for_status
from proxystore.endpoint.protocol import Request
from proxystore.endpoint.protocol import Status
from proxystore.endpoint.protocol import STATUS_ERRORS
from proxystore.endpoint.protocol import Versions


def test_versions_current() -> None:
    versions = Versions.current()
    assert versions.proxystore == proxystore.__version__
    assert versions.python == platform.python_version()


def test_preamble_round_trip() -> None:
    assert Preamble.unpack(Preamble().pack()).version == PROTOCOL_VERSION
    assert Preamble.unpack(Preamble(42).pack()).version == 42


def test_preamble_bad_magic() -> None:
    with pytest.raises(EndpointProtocolError, match='Expected connection'):
        Preamble.unpack(b'GET /x')


def _json(meta: dict[str, Any]) -> bytes:
    return json.dumps(meta).encode()


def test_message_round_trip() -> None:
    meta = Request(key='abc').encode()
    message = Message(Op.SET, meta).pack_head(100)

    header = Header.unpack(message[: Header.SIZE])
    assert header == Header(Op.SET, 0, 0, len(meta), 100)
    assert Request.decode(message[Header.SIZE :]) == Request(key='abc')


def test_message_no_meta() -> None:
    message = Message(Op.GET).pack_head()
    header = Header.unpack(message)
    assert header.meta_len == 0
    assert header.data_len == 0
    # Empty metadata is an empty object
    assert Request.decode(b'') == Request()


def test_header_meta_too_large() -> None:
    header = Header(Op.GET, 0, 0, MAX_META_SIZE + 1, 0).pack()
    with pytest.raises(EndpointProtocolError, match='exceeds the maximum'):
        Header.unpack(header)


@pytest.mark.parametrize(
    ('buffer', 'error'),
    (
        (b'{', 'Invalid JSON'),
        (b'\xff\xfe', 'Invalid JSON'),
        (b'[1, 2]', 'Input should be an object'),
    ),
)
def test_decode_meta_invalid(buffer: bytes, error: str) -> None:
    with pytest.raises(EndpointProtocolError, match=error):
        Request.decode(buffer)


def _versions(proxystore: str, python: str) -> Versions:
    return Versions(proxystore=proxystore, python=python)


@pytest.mark.parametrize(
    ('client', 'endpoint', 'expected'),
    (
        # Same versions
        (_versions('1.0.0', '3.12.4'), _versions('1.0.0', '3.12.4'), []),
        # Python patch versions are compatible
        (_versions('1.0.0', '3.12.4'), _versions('1.0.0', '3.12.9'), []),
        (
            _versions('1.0.0', '3.12.4'),
            _versions('1.0.1', '3.12.4'),
            ['ProxyStore 1.0.0 (client) vs. 1.0.1 (endpoint)'],
        ),
        (
            _versions('1.0.0', '3.12.4'),
            _versions('1.0.0', '3.13.0'),
            ['Python 3.12.4 (client) vs. 3.13.0 (endpoint)'],
        ),
        (
            _versions('1.0.0', '3.12.4'),
            _versions('1.0.1', '3.13.0'),
            [
                'ProxyStore 1.0.0 (client) vs. 1.0.1 (endpoint)',
                'Python 3.12.4 (client) vs. 3.13.0 (endpoint)',
            ],
        ),
    ),
)
def test_versions_mismatches(
    client: Versions,
    endpoint: Versions,
    expected: list[str],
) -> None:
    assert client.mismatches(endpoint) == expected


_NONCE = os.urandom(NONCE_SIZE)
_PROOF = os.urandom(32)
_VERSIONS = _versions('1.0.0', '3.12.4')
_ID = EndpointId.random()


@pytest.mark.parametrize(
    'meta',
    (
        Hello(nonce=_NONCE, versions=_VERSIONS),
        Challenge(nonce=_NONCE, proof=_PROOF),
        Auth(proof=_PROOF),
        EndpointInfo(id=_ID, name='n', versions=_VERSIONS, max_object_size=1),
        EndpointInfo(
            id=_ID,
            name='n',
            versions=_VERSIONS,
            max_object_size=None,
        ),
        Request(),
        Request(key='key'),
        Request(target=_ID),
        Request(key='key', target=_ID),
        ExistsResult(exists=True),
        PingResult(),
        PingResult(
            peer_rtt_ms=12.5,
            relayed=True,
            remote_addr='https://relay.example.com',
            path_rtt_ms=10,
        ),
        ErrorInfo(error='failed'),
    ),
)
def test_meta_round_trip(meta: Meta) -> None:
    assert type(meta).decode(meta.encode()) == meta


def test_meta_encoding() -> None:
    # Bytes are hex-encoded and unknown fields are ignored
    meta = Auth(proof=_PROOF).encode()
    assert json.loads(meta) == {'proof': _PROOF.hex()}
    assert (
        Auth.decode(_json({'proof': _PROOF.hex(), 'new': 1})).proof == _PROOF
    )
    # IDs are normalized
    assert Request.decode(_json({'target': _ID.upper()})).target == _ID
    # Integers are valid floats
    assert PingResult.decode(_json({'peer_rtt_ms': 1})).peer_rtt_ms == 1.0


_HELLO = Hello(nonce=_NONCE, versions=_VERSIONS).model_dump(mode='json')
_INFO = EndpointInfo(
    id=_ID,
    name='n',
    versions=_VERSIONS,
    max_object_size=1,
).model_dump(mode='json')


@pytest.mark.parametrize(
    ('model', 'meta', 'field'),
    (
        (Hello, {'versions': _HELLO['versions']}, 'nonce'),
        (Hello, {**_HELLO, 'nonce': 42}, 'nonce'),
        (Hello, {**_HELLO, 'nonce': 'x'}, 'nonce'),
        # Nonces must have the expected size
        (Hello, {**_HELLO, 'nonce': _NONCE[:-1].hex()}, 'nonce'),
        (
            Hello,
            {**_HELLO, 'versions': {'python': '3.12.4'}},
            'versions.proxystore',
        ),
        (Challenge, {'nonce': _NONCE.hex()}, 'proof'),
        (Auth, {'proof': None}, 'proof'),
        (EndpointInfo, {}, 'id'),
        (EndpointInfo, {**_INFO, 'id': 'x'}, 'id'),
        (EndpointInfo, {**_INFO, 'max_object_size': '1'}, 'max_object_size'),
        (Request, {'key': ''}, 'key'),
        (Request, {'key': 42}, 'key'),
        (Request, {'key': 'key', 'target': 42}, 'target'),
        (Request, {'key': 'key', 'target': 'not-an-id'}, 'target'),
        (ExistsResult, {}, 'exists'),
        (ExistsResult, {'exists': 'yes'}, 'exists'),
        (PingResult, {'relayed': 'yes'}, 'relayed'),
        (PingResult, {'path_rtt_ms': 1.5}, 'path_rtt_ms'),
    ),
)
def test_meta_malformed(
    model: type[Meta],
    meta: dict[str, Any],
    field: str,
) -> None:
    name = model.__name__
    with pytest.raises(
        EndpointProtocolError,
        match=f"Malformed {name} message: missing or invalid '{field}'",
    ):
        model.decode(_json(meta))


def test_error_message() -> None:
    assert Message.error(Status.ERROR, 'failed').error_message == 'failed'
    assert Message(Status.ERROR).error_message == 'no error message provided'
    assert Message(Status.ERROR, b'[]').error_message == (
        'no error message provided'
    )


@pytest.mark.parametrize('status', (Status.OK, Status.NOT_FOUND))
def test_raise_for_status_ok(status: Status) -> None:
    assert raise_for_status(Message(status), Op.GET) == status


@pytest.mark.parametrize(('status', 'error'), tuple(STATUS_ERRORS.items()))
def test_raise_for_status_errors(
    status: Status,
    error: type[EndpointError],
) -> None:
    response = Message.error(status, 'failed')
    with pytest.raises(error, match=f'Peer x returned {status.name}') as e:
        raise_for_status(response, Op.SET, source='Peer x')
    # The most specific type is raised
    assert type(e.value) is error
    assert 'for SET request: failed' in str(e.value)


def test_raise_for_status_unknown() -> None:
    with pytest.raises(EndpointRequestError, match='no error message'):
        raise_for_status(Message(Status.ERROR), Op.GET)
    with pytest.raises(EndpointRequestError, match='unknown status code 99'):
        raise_for_status(Message(99), Op.GET)


def test_every_error_status_has_an_exception() -> None:
    ok = {Status.OK, Status.NOT_FOUND}
    assert set(STATUS_ERRORS) == set(Status) - ok


@pytest.mark.parametrize(('status', 'error'), tuple(STATUS_ERRORS.items()))
def test_error_status_round_trip(
    status: Status,
    error: type[EndpointError],
) -> None:
    response = Message.from_error(error('failed'))
    assert response == Message.error(status, 'failed')
    with pytest.raises(error):
        raise_for_status(response, Op.GET)


def test_error_status_subclass_and_unknown() -> None:
    assert error_status(PeerConnectionTimeoutError()) == (
        Status.PEER_UNAVAILABLE
    )
    assert error_status(PeerError()) == Status.ERROR
    assert error_status(RuntimeError()) == Status.ERROR


@pytest.mark.parametrize(
    ('meta', 'data'),
    (
        (b'', b''),
        (b'{"key":"k"}', b''),
        (b'', b'data'),
        (b'{"key":"k"}', b'data'),
    ),
)
def test_message_reader(meta: bytes, data: bytes) -> None:
    message = Message(Op.SET, meta, data, request_id=7)
    buffer = message.pack_head() + data
    reader = MessageReader()
    sizes = []
    while not reader.done:
        size = reader.size
        sizes.append(size)
        reader.feed(buffer[:size])
        buffer = buffer[size:]
    assert buffer == b''
    assert reader.size == 0
    assert reader.message == message
    # Empty parts of the message are skipped
    assert 0 not in sizes


def test_message_reader_max_data_size() -> None:
    reader = MessageReader(max_data_size=3)
    buffer = Message(Op.SET, b'{"key":"k"}', request_id=3).pack_head(4)
    reader.feed(buffer[: Header.SIZE])
    with pytest.raises(ObjectSizeExceededError, match='4 bytes'):
        reader.feed(buffer[Header.SIZE :])
    # The header is available to respond to the request
    assert reader.header.request_id == 3
    assert not reader.done


def test_message_reader_errors() -> None:
    reader = MessageReader()
    with pytest.raises(RuntimeError, match='header'):
        _ = reader.header
    with pytest.raises(RuntimeError, match='message'):
        _ = reader.message
    with pytest.raises(ValueError, match='Expected'):
        reader.feed(b'x')
    reader.feed(Message(Status.OK).pack_head())
    assert reader.done
    with pytest.raises(RuntimeError, match='already been read'):
        reader.feed(b'')


def test_alpn_matches_protocol_version() -> None:
    assert f'proxystore/{PROTOCOL_VERSION}'.encode() == ALPN


def test_versions_mismatch_warning() -> None:
    client = _versions('1.0.0', '3.12.4')
    assert client.mismatch_warning(client) is None
    warning = client.mismatch_warning(_versions('1.0.1', '3.12.4'))
    assert warning is not None
    assert warning.startswith('ProxyStore 1.0.0 (client) vs. 1.0.1 (endpoint)')
    assert 'may fail to deserialize' in warning


def test_handshake_reader() -> None:
    reader = HandshakeReader()
    reader.feed(Message(Status.OK, b'{}').pack_head()[: Header.SIZE])
    reader.feed(b'{}')
    assert reader.message == Message(Status.OK, b'{}')

    reader = HandshakeReader()
    head = Message(Op.HELLO, b'{}').pack_head(1)
    reader.feed(head[: Header.SIZE])
    with pytest.raises(EndpointProtocolError, match='code 1 contains data'):
        reader.feed(head[Header.SIZE :])
