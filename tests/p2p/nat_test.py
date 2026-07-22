from __future__ import annotations

import asyncio
import logging
import socket
import struct
from typing import Any
from unittest import mock

import pytest

from proxystore.p2p.nat import _decode_response
from proxystore.p2p.nat import _encode_request
from proxystore.p2p.nat import _local_address
from proxystore.p2p.nat import _MAGIC_COOKIE
from proxystore.p2p.nat import _resolve_servers
from proxystore.p2p.nat import _STUN_SERVERS
from proxystore.p2p.nat import check_nat
from proxystore.p2p.nat import check_nat_and_log
from proxystore.p2p.nat import NatMapping
from proxystore.p2p.nat import Result

TRANSACTION_ID = b'0123456789ab'


def encode_attribute(attribute_type: int, value: bytes) -> bytes:
    padding = b'\x00' * (-len(value) % 4)
    return struct.pack('>HH', attribute_type, len(value)) + value + padding


def encode_address(ip: str, port: int, xor: bool) -> bytes:
    packed_ip = socket.inet_aton(ip)
    if xor:
        port ^= _MAGIC_COOKIE >> 16
        cookie = struct.pack('>I', _MAGIC_COOKIE)
        packed_ip = bytes(
            a ^ b for a, b in zip(packed_ip, cookie, strict=True)
        )
    return b'\x00\x01' + struct.pack('>H', port) + packed_ip


def encode_response(
    ip: str = '192.168.1.1',
    port: int = 1234,
    *,
    transaction_id: bytes = TRANSACTION_ID,
    message_type: int = 0x0101,
    cookie: int = _MAGIC_COOKIE,
    attributes: bytes | None = None,
) -> bytes:
    if attributes is None:
        attributes = encode_attribute(0x0020, encode_address(ip, port, True))
    header = struct.pack(
        '>HHI12s',
        message_type,
        len(attributes),
        cookie,
        transaction_id,
    )
    return header + attributes


def test_encode_request() -> None:
    request = _encode_request(TRANSACTION_ID)
    message_type, length, cookie, transaction_id = struct.unpack(
        '>HHI12s',
        request,
    )

    assert message_type == 0x0001
    assert length == 0
    assert cookie == _MAGIC_COOKIE
    assert transaction_id == TRANSACTION_ID


def test_decode_response_xor_mapped_address() -> None:
    decoded = _decode_response(encode_response('93.184.216.34', 40000))

    assert decoded is not None
    assert decoded == (TRANSACTION_ID, ('93.184.216.34', 40000))


def test_decode_response_mapped_address_fallback() -> None:
    # Servers which only send the pre-RFC 5389 attribute are still usable.
    attributes = encode_attribute(
        0x0001,
        encode_address('93.184.216.34', 40000, False),
    )
    decoded = _decode_response(encode_response(attributes=attributes))

    assert decoded == (TRANSACTION_ID, ('93.184.216.34', 40000))


def test_decode_response_prefers_xor_mapped_address() -> None:
    # A server sending both must be read via the XOR variant since middleboxes
    # may have rewritten the plain one.
    attributes = encode_attribute(
        0x0001,
        encode_address('10.0.0.1', 1, False),
    ) + encode_attribute(
        0x0020,
        encode_address('93.184.216.34', 40000, True),
    )
    decoded = _decode_response(encode_response(attributes=attributes))

    assert decoded == (TRANSACTION_ID, ('93.184.216.34', 40000))


def test_decode_response_skips_unknown_attributes() -> None:
    attributes = encode_attribute(0x8022, b'test server') + encode_attribute(
        0x0020,
        encode_address('93.184.216.34', 40000, True),
    )
    decoded = _decode_response(encode_response(attributes=attributes))

    assert decoded == (TRANSACTION_ID, ('93.184.216.34', 40000))


@pytest.mark.parametrize(
    'data',
    (
        # Truncated header.
        b'\x01\x01\x00\x00',
        # Binding error response rather than success.
        encode_response(message_type=0x0111),
        # Response from something which is not a STUN server.
        encode_response(cookie=0xDEADBEEF),
        # Success carrying no address.
        encode_response(attributes=b''),
        # IPv6 address, which this check does not support.
        encode_response(attributes=encode_attribute(0x0020, b'\x00\x02' * 9)),
    ),
)
def test_decode_response_invalid(data: bytes) -> None:
    assert _decode_response(data) is None


async def test_resolve_servers_deduplicates_by_ip() -> None:
    # Servers sharing an IP provide only one measurement point, so keeping
    # both would make an address-dependent NAT look endpoint-independent.
    def getaddrinfo(host: str, port: int, **kwargs: Any) -> list[Any]:
        ip = '1.1.1.1' if host in ('a', 'b') else '2.2.2.2'
        return [(socket.AF_INET, socket.SOCK_DGRAM, 17, '', (ip, port))]

    with mock.patch.object(
        asyncio.get_running_loop(),
        'getaddrinfo',
        side_effect=getaddrinfo,
    ):
        resolved = await _resolve_servers((('a', 1), ('b', 2), ('c', 3)))

    assert resolved == [('1.1.1.1', 1), ('2.2.2.2', 3)]


async def test_resolve_servers_ignores_failures() -> None:
    async def getaddrinfo(host: str, port: int, **kwargs: Any) -> list[Any]:
        if host == 'bad':
            raise socket.gaierror('test error')
        return [(socket.AF_INET, socket.SOCK_DGRAM, 17, '', ('1.1.1.1', port))]

    with mock.patch.object(
        asyncio.get_running_loop(),
        'getaddrinfo',
        side_effect=getaddrinfo,
    ):
        resolved = await _resolve_servers((('bad', 1), ('good', 2)))

    assert resolved == [('1.1.1.1', 2)]


class MockStunServers:
    """Replies to binding requests with a configurable external address.

    Args:
        addresses: External address to reflect back per server IP. A value of
            `None` means that server does not respond.
    """

    def __init__(self, addresses: dict[str, tuple[str, int] | None]) -> None:
        self.addresses = addresses
        self.transport = mock.MagicMock()
        self.transport.sendto.side_effect = self._sendto
        self.protocol: Any = None

    def _sendto(self, data: bytes, server: tuple[str, int]) -> None:
        address = self.addresses[server[0]]
        if address is None:
            return
        transaction_id = data[8:20]
        self.protocol.datagram_received(
            encode_response(*address, transaction_id=transaction_id),
            server,
        )

    async def create_datagram_endpoint(
        self,
        protocol_factory: Any,
        **kwargs: Any,
    ) -> tuple[Any, Any]:
        self.protocol = protocol_factory()
        return self.transport, self.protocol


def patch_stun(
    addresses: dict[str, tuple[str, int] | None],
    local_ip: str = '10.0.0.1',
) -> Any:
    servers = [(ip, 3478) for ip in addresses]
    mock_servers = MockStunServers(addresses)
    loop = asyncio.get_running_loop()
    return mock.patch.multiple(
        'proxystore.p2p.nat',
        _resolve_servers=mock.AsyncMock(return_value=servers),
        _local_address=mock.MagicMock(return_value=local_ip),
    ), mock.patch.object(
        loop,
        'create_datagram_endpoint',
        side_effect=mock_servers.create_datagram_endpoint,
    )


async def test_check_nat_endpoint_independent() -> None:
    # Every server reflects the same address, so one mapping serves all peers.
    patches = patch_stun(
        {
            '1.1.1.1': ('93.184.216.34', 40000),
            '2.2.2.2': ('93.184.216.34', 40000),
        },
    )
    with patches[0], patches[1]:
        result = await check_nat()

    assert result.mapping == NatMapping.EndpointIndependent
    assert result.external_ip == '93.184.216.34'
    assert result.external_port == 40000
    assert result.hole_punching_likely


async def test_check_nat_address_dependent() -> None:
    # A different port per server means a peer cannot use the address it is
    # told about. Regression test for #729, which reported such NATs as
    # full-cone.
    patches = patch_stun(
        {
            '1.1.1.1': ('93.184.216.34', 40000),
            '2.2.2.2': ('93.184.216.34', 50000),
        },
    )
    with patches[0], patches[1]:
        result = await check_nat()

    assert result.mapping == NatMapping.AddressDependent
    assert not result.hole_punching_likely


async def test_check_nat_no_nat() -> None:
    # The reflected address matching the local address means no NAT at all.
    patches = patch_stun(
        {
            '1.1.1.1': ('93.184.216.34', 40000),
            '2.2.2.2': ('93.184.216.34', 40000),
        },
        local_ip='93.184.216.34',
    )
    with patches[0], patches[1]:
        result = await check_nat()

    assert result.mapping == NatMapping.NoNat
    assert result.hole_punching_likely


async def test_check_nat_requires_two_responses() -> None:
    # One response cannot distinguish the two behaviors, so it must not be
    # reported as though it could. Unanswered requests are retransmitted, so
    # this also exercises every retransmission round.
    patches = patch_stun(
        {'1.1.1.1': ('93.184.216.34', 40000), '2.2.2.2': None},
    )
    with (
        patches[0],
        patches[1],
        mock.patch(
            'proxystore.p2p.nat._RETRANSMIT_DELAYS',
            (0.001, 0.001, 0.001),
        ),
    ):
        with pytest.raises(RuntimeError, match='Only 1 of 2 STUN servers'):
            await check_nat()


async def test_check_nat_stops_at_timeout() -> None:
    # A network which blocks STUN must fail within the timeout rather than
    # running every retransmission round.
    patches = patch_stun({'1.1.1.1': None, '2.2.2.2': None})
    with patches[0], patches[1]:
        with pytest.raises(RuntimeError, match='Only 0 of 2 STUN servers'):
            await check_nat(timeout=0)


def test_local_address() -> None:
    # 192.0.2.1 is TEST-NET-1 so connecting resolves a route without sending.
    assert _local_address(('192.0.2.1', 3478)) != '0.0.0.0'


async def test_check_nat_requires_two_servers() -> None:
    with mock.patch(
        'proxystore.p2p.nat._resolve_servers',
        mock.AsyncMock(return_value=[('1.1.1.1', 3478)]),
    ):
        with pytest.raises(RuntimeError, match='Only 1 STUN servers'):
            await check_nat()


def test_stun_servers_are_distinct_hosts() -> None:
    hosts = [host for host, _ in _STUN_SERVERS]

    assert len(_STUN_SERVERS) >= 2
    assert len(set(hosts)) == len(hosts)


async def test_check_nat_and_log_hole_punching_likely(caplog) -> None:
    caplog.set_level(logging.INFO)

    result = Result(
        NatMapping.EndpointIndependent,
        '93.184.216.34',
        1234,
        True,
    )
    with mock.patch(
        'proxystore.p2p.nat.check_nat',
        mock.AsyncMock(return_value=result),
    ):
        await check_nat_and_log()

    messages = [r.message for r in caplog.records]
    assert any('Endpoint-independent mapping' in m for m in messages)
    assert any('External IP:    93.184.216.34' in m for m in messages)
    assert any('External Port:  1234' in m for m in messages)
    assert any(m.startswith('NAT traversal for peer') for m in messages)


async def test_check_nat_and_log_address_dependent(caplog) -> None:
    caplog.set_level(logging.INFO)

    result = Result(NatMapping.AddressDependent, '93.184.216.34', 1234, False)
    with mock.patch(
        'proxystore.p2p.nat.check_nat',
        mock.AsyncMock(return_value=result),
    ):
        await check_nat_and_log()

    messages = [r.message for r in caplog.records]
    assert any('Address-dependent mapping' in m for m in messages)
    assert any('will not work reliably' in m for m in messages)


async def test_check_nat_and_log_failure(caplog) -> None:
    caplog.set_level(logging.INFO)

    with mock.patch(
        'proxystore.p2p.nat.check_nat',
        mock.AsyncMock(side_effect=RuntimeError('test error')),
    ):
        await check_nat_and_log()

    assert any(
        r.message.startswith('Failed to determine NAT behavior: test error')
        for r in caplog.records
    )
