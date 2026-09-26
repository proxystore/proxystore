from __future__ import annotations

import os
import platform
import uuid
from typing import Any

import pytest

import proxystore
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.protocol import Auth
from proxystore.endpoint.protocol import Challenge
from proxystore.endpoint.protocol import decode_meta
from proxystore.endpoint.protocol import encode_meta
from proxystore.endpoint.protocol import EndpointInfo
from proxystore.endpoint.protocol import HEADER
from proxystore.endpoint.protocol import Header
from proxystore.endpoint.protocol import Hello
from proxystore.endpoint.protocol import local_versions
from proxystore.endpoint.protocol import MAX_META_SIZE
from proxystore.endpoint.protocol import NONCE_SIZE
from proxystore.endpoint.protocol import Op
from proxystore.endpoint.protocol import pack_message
from proxystore.endpoint.protocol import pack_preamble
from proxystore.endpoint.protocol import PROTOCOL_VERSION
from proxystore.endpoint.protocol import unpack_header
from proxystore.endpoint.protocol import unpack_preamble
from proxystore.endpoint.protocol import version_mismatches
from proxystore.endpoint.protocol import Versions


def test_local_versions() -> None:
    versions = local_versions()
    assert versions.proxystore == proxystore.__version__
    assert versions.python == platform.python_version()


def test_preamble_round_trip() -> None:
    assert unpack_preamble(pack_preamble()) == PROTOCOL_VERSION
    assert unpack_preamble(pack_preamble(42)) == 42


def test_preamble_bad_magic() -> None:
    with pytest.raises(EndpointProtocolError, match='Expected connection'):
        unpack_preamble(b'GET /x')


def test_message_round_trip() -> None:
    meta = {'key': 'abc', 'endpoint': None}
    message = pack_message(Op.SET, meta, data_len=100)

    header = unpack_header(message[: HEADER.size])
    assert header == Header(Op.SET, 0, len(encode_meta(meta)), 100)
    assert decode_meta(message[HEADER.size :]) == meta


def test_message_no_meta() -> None:
    message = pack_message(Op.GET)
    header = unpack_header(message)
    assert header.meta_len == 0
    assert header.data_len == 0
    assert decode_meta(b'') == {}


def test_header_meta_too_large() -> None:
    header = HEADER.pack(Op.GET, 0, MAX_META_SIZE + 1, 0)
    with pytest.raises(EndpointProtocolError, match='exceeds the maximum'):
        unpack_header(header)


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
def test_version_mismatches(
    client: Versions,
    endpoint: Versions,
    expected: list[str],
) -> None:
    assert version_mismatches(client, endpoint) == expected


_NONCE = os.urandom(NONCE_SIZE)
_PROOF = os.urandom(32)
_VERSIONS = Versions('1.0.0', '3.12.4')


@pytest.mark.parametrize(
    'message',
    (
        Hello(_NONCE, _VERSIONS),
        Challenge(_NONCE, _PROOF),
        Auth(_PROOF),
        EndpointInfo(uuid.uuid4(), 'name', _VERSIONS, 100),
        EndpointInfo(uuid.uuid4(), 'name', _VERSIONS, None),
    ),
)
def test_handshake_message_round_trip(
    message: Hello | Challenge | Auth | EndpointInfo,
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
        (EndpointInfo, {}, 'uuid'),
        (
            EndpointInfo,
            {
                **EndpointInfo(uuid.uuid4(), 'n', _VERSIONS, 1).to_meta(),
                'uuid': 'x',
            },
            'uuid',
        ),
        (
            EndpointInfo,
            {
                **EndpointInfo(uuid.uuid4(), 'n', _VERSIONS, 1).to_meta(),
                'max_object_size': '1',
            },
            'max_object_size',
        ),
    ),
)
def test_handshake_message_malformed(
    message: type[Hello | Challenge | Auth | EndpointInfo],
    meta: dict[str, Any],
    field: str,
) -> None:
    with pytest.raises(EndpointProtocolError, match=f"invalid '{field}'"):
        message.from_meta(meta)
