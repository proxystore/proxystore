from __future__ import annotations

import os
import platform
from typing import Any

import pytest

import proxystore
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.exceptions import ObjectSizeExceededError
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.protocol import Auth
from proxystore.endpoint.protocol import Challenge
from proxystore.endpoint.protocol import check_response
from proxystore.endpoint.protocol import decode_meta
from proxystore.endpoint.protocol import encode_meta
from proxystore.endpoint.protocol import EndpointInfo
from proxystore.endpoint.protocol import exists_from_meta
from proxystore.endpoint.protocol import Header
from proxystore.endpoint.protocol import Hello
from proxystore.endpoint.protocol import MAX_META_SIZE
from proxystore.endpoint.protocol import Message
from proxystore.endpoint.protocol import NONCE_SIZE
from proxystore.endpoint.protocol import Op
from proxystore.endpoint.protocol import pack_message
from proxystore.endpoint.protocol import PingResult
from proxystore.endpoint.protocol import Preamble
from proxystore.endpoint.protocol import PROTOCOL_VERSION
from proxystore.endpoint.protocol import Request
from proxystore.endpoint.protocol import Status
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


def test_message_round_trip() -> None:
    meta = {'key': 'abc', 'endpoint': None}
    message = pack_message(Op.SET, meta, data_len=100)

    header = Header.unpack(message[: Header.SIZE])
    assert header == Header(Op.SET, 0, len(encode_meta(meta)), 100)
    assert decode_meta(message[Header.SIZE :]) == meta


def test_message_no_meta() -> None:
    message = pack_message(Op.GET)
    header = Header.unpack(message)
    assert header.meta_len == 0
    assert header.data_len == 0
    assert decode_meta(b'') == {}


def test_header_meta_too_large() -> None:
    header = Header(Op.GET, 0, MAX_META_SIZE + 1, 0).pack()
    with pytest.raises(EndpointProtocolError, match='exceeds the maximum'):
        Header.unpack(header)


@pytest.mark.parametrize('buffer', (b'{', b'\xff\xfe', b'[1, 2]'))
def test_decode_meta_invalid(buffer: bytes) -> None:
    with pytest.raises(EndpointProtocolError):
        decode_meta(buffer)


@pytest.mark.parametrize(
    ('client', 'endpoint', 'expected'),
    (
        # Same versions
        (Versions('1.0.0', '3.12.4'), Versions('1.0.0', '3.12.4'), []),
        # Python patch versions are compatible
        (Versions('1.0.0', '3.12.4'), Versions('1.0.0', '3.12.9'), []),
        (
            Versions('1.0.0', '3.12.4'),
            Versions('1.0.1', '3.12.4'),
            ['ProxyStore 1.0.0 (client) vs. 1.0.1 (endpoint)'],
        ),
        (
            Versions('1.0.0', '3.12.4'),
            Versions('1.0.0', '3.13.0'),
            ['Python 3.12.4 (client) vs. 3.13.0 (endpoint)'],
        ),
        (
            Versions('1.0.0', '3.12.4'),
            Versions('1.0.1', '3.13.0'),
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
_VERSIONS = Versions('1.0.0', '3.12.4')


@pytest.mark.parametrize(
    'message',
    (
        Hello(_NONCE, _VERSIONS),
        Challenge(_NONCE, _PROOF),
        Auth(_PROOF),
        EndpointInfo(EndpointId.random(), 'name', _VERSIONS, 100),
        EndpointInfo(EndpointId.random(), 'name', _VERSIONS, None),
        Request('key'),
        Request('key', EndpointId.random()),
    ),
)
def test_message_meta_round_trip(
    message: Hello | Challenge | Auth | EndpointInfo | Request,
) -> None:
    meta = decode_meta(encode_meta(message.to_meta()))
    assert type(message).from_meta(meta) == message


@pytest.mark.parametrize(
    ('message', 'meta', 'field'),
    (
        (Hello, {'python': '3.12.4', 'proxystore': '1.0.0'}, 'nonce'),
        (Hello, {**Hello(_NONCE, _VERSIONS).to_meta(), 'nonce': 42}, 'nonce'),
        (Hello, {**Hello(_NONCE, _VERSIONS).to_meta(), 'nonce': 'x'}, 'nonce'),
        # Nonces must have the expected size
        (Hello, Hello(_NONCE[:-1], _VERSIONS).to_meta(), 'nonce'),
        (Hello, {'nonce': _NONCE.hex(), 'python': '3.12.4'}, 'proxystore'),
        (Challenge, {'nonce': _NONCE.hex()}, 'proof'),
        (Auth, {'proof': None}, 'proof'),
        (EndpointInfo, {}, 'id'),
        (
            EndpointInfo,
            {
                **EndpointInfo(
                    EndpointId.random(), 'n', _VERSIONS, 1
                ).to_meta(),
                'id': 'x',
            },
            'id',
        ),
        (
            EndpointInfo,
            {
                **EndpointInfo(
                    EndpointId.random(), 'n', _VERSIONS, 1
                ).to_meta(),
                'max_object_size': '1',
            },
            'max_object_size',
        ),
        (Request, {'endpoint': None}, 'key'),
        (Request, {'key': '', 'endpoint': None}, 'key'),
        (Request, {'key': 'key'}, 'endpoint'),
        (Request, {'key': 'key', 'endpoint': 42}, 'endpoint'),
        (Request, {'key': 'key', 'endpoint': 'not-an-id'}, 'endpoint'),
    ),
)
def test_message_meta_malformed(
    message: type[Hello | Challenge | Auth | EndpointInfo | Request],
    meta: dict[str, Any],
    field: str,
) -> None:
    with pytest.raises(EndpointProtocolError, match=f"invalid '{field}'"):
        message.from_meta(meta)


@pytest.mark.parametrize(
    'message',
    (
        PingResult(),
        PingResult(12.5, True, 'https://relay.example.com', 10),
        PingResult(1, False, '1.2.3.4:5', 0),
    ),
)
def test_ping_meta_round_trip(message: PingResult) -> None:
    meta = decode_meta(encode_meta(message.to_meta()))
    assert type(message).from_meta(meta) == message


@pytest.mark.parametrize(
    ('message', 'meta', 'field'),
    (
        (PingResult, {}, 'peer_rtt_ms'),
        (PingResult, {**PingResult().to_meta(), 'relayed': 'yes'}, 'relayed'),
        (
            PingResult,
            {**PingResult().to_meta(), 'path_rtt_ms': 1.5},
            'path_rtt_ms',
        ),
    ),
)
def test_ping_meta_malformed(
    message: type[PingResult],
    meta: dict[str, Any],
    field: str,
) -> None:
    with pytest.raises(EndpointProtocolError, match=f"invalid '{field}'"):
        message.from_meta(meta)


@pytest.mark.parametrize(
    'request_',
    (Request(), Request(endpoint=EndpointId.random()), Request('key')),
)
def test_request_optional_key_round_trip(request_: Request) -> None:
    meta = decode_meta(encode_meta(request_.to_meta()))
    assert Request.from_meta(meta) == request_


@pytest.mark.parametrize('status', (Status.OK, Status.NOT_FOUND))
def test_check_response_ok(status: Status) -> None:
    response = Message(status)
    assert (
        check_response(response, Op.GET, source='Endpoint', error=KeyError)
        == status
    )


def test_check_response_errors() -> None:
    def _check(response: Message) -> None:
        check_response(response, Op.SET, source='Peer x', error=KeyError)

    with pytest.raises(ObjectSizeExceededError, match='Peer x returned TOO'):
        _check(Message.error(Status.TOO_LARGE, 'too big'))
    with pytest.raises(KeyError, match='ERROR for SET request: failed'):
        _check(Message.error(Status.ERROR, 'failed'))
    with pytest.raises(KeyError, match='no error message provided'):
        _check(Message(Status.BAD_REQUEST))
    with pytest.raises(EndpointProtocolError, match='unknown status code 99'):
        _check(Message(99))


def test_exists_from_meta() -> None:
    assert exists_from_meta({'exists': True})
    assert not exists_from_meta({'exists': False})
    for meta in ({}, {'exists': 'yes'}):
        with pytest.raises(EndpointProtocolError, match='Malformed EXISTS'):
            exists_from_meta(meta)
