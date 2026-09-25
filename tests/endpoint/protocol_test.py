from __future__ import annotations

import platform

import pytest

import proxystore
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.protocol import decode_meta
from proxystore.endpoint.protocol import encode_meta
from proxystore.endpoint.protocol import HEADER
from proxystore.endpoint.protocol import Header
from proxystore.endpoint.protocol import local_versions
from proxystore.endpoint.protocol import MAX_META_SIZE
from proxystore.endpoint.protocol import Op
from proxystore.endpoint.protocol import pack_message
from proxystore.endpoint.protocol import pack_preamble
from proxystore.endpoint.protocol import PROTOCOL_VERSION
from proxystore.endpoint.protocol import unpack_header
from proxystore.endpoint.protocol import unpack_preamble
from proxystore.endpoint.protocol import version_mismatches


def test_local_versions() -> None:
    versions = local_versions()
    assert versions['proxystore'] == proxystore.__version__
    assert versions['python'] == platform.python_version()


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
        (
            {'proxystore': '1.0.0', 'python': '3.12.4'},
            {'proxystore': '1.0.0', 'python': '3.12.4'},
            [],
        ),
        # Python patch versions are compatible
        (
            {'proxystore': '1.0.0', 'python': '3.12.4'},
            {'proxystore': '1.0.0', 'python': '3.12.9'},
            [],
        ),
        (
            {'proxystore': '1.0.0', 'python': '3.12.4'},
            {'proxystore': '1.0.1', 'python': '3.12.4'},
            ['ProxyStore 1.0.0 (client) vs. 1.0.1 (endpoint)'],
        ),
        (
            {'proxystore': '1.0.0', 'python': '3.12.4'},
            {'proxystore': '1.0.0', 'python': '3.13.0'},
            ['Python 3.12.4 (client) vs. 3.13.0 (endpoint)'],
        ),
        (
            {'proxystore': '1.0.0', 'python': '3.12.4'},
            {},
            [
                'ProxyStore 1.0.0 (client) vs. unknown (endpoint)',
                'Python 3.12.4 (client) vs. unknown (endpoint)',
            ],
        ),
    ),
)
def test_version_mismatches(
    client: dict[str, str],
    endpoint: dict[str, str],
    expected: list[str],
) -> None:
    assert version_mismatches(client, endpoint) == expected
