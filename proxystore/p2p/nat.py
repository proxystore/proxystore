"""Tools for checking NAT mapping behavior using STUN.

This module implements the mapping behavior discovery procedure of
[RFC 5780](https://datatracker.ietf.org/doc/html/rfc5780){target=_blank}
using [RFC 5389](https://datatracker.ietf.org/doc/html/rfc5389){target=_blank}
binding requests.

The classic NAT taxonomy of RFC 3489 (full-cone, restricted-cone, and so on)
was deprecated because real NATs do not fall into those categories: mapping
behavior and filtering behavior are independent. Only mapping behavior affects
whether NAT traversal works. A NAT which assigns the same external address
regardless of the destination (endpoint-independent mapping) can be traversed
by hole-punching, even when it filters unsolicited traffic, because the relay
server coordinates both peers to send simultaneously. A NAT which assigns a
different external address per destination (address-dependent mapping,
historically "symmetric") cannot, because the address a peer learns is not the
address it must send to.

Determining filtering behavior would require the RFC 3489 CHANGE-REQUEST
attribute, which needs a STUN server listening on two IP addresses. Such
servers are increasingly rare, and the answer would not change the advice
given here, so this module does not use them.
"""

from __future__ import annotations

import asyncio
import contextlib
import enum
import logging
import secrets
import socket
import struct
from typing import NamedTuple

logger = logging.getLogger(__name__)

# Determining mapping behavior requires binding requests to at least two
# servers on *different* IP addresses. Servers which resolve to the same
# address are deduplicated at runtime, so entries here are chosen across
# distinct operators rather than for redundancy.
_STUN_SERVERS = (
    ('stun.l.google.com', 19302),
    ('stun.nextcloud.com', 3478),
    ('stun.hot-chilli.net', 3478),
    ('stun.m-online.net', 3478),
    ('stun.dus.net', 3478),
)

_MAGIC_COOKIE = 0x2112A442
_BINDING_REQUEST = 0x0001
_BINDING_SUCCESS = 0x0101
_MAPPED_ADDRESS = 0x0001
_XOR_MAPPED_ADDRESS = 0x0020
_IPV4_FAMILY = 0x01
_TRANSACTION_ID_BYTES = 12
_HEADER_FORMAT = '>HHI12s'
_HEADER_BYTES = struct.calcsize(_HEADER_FORMAT)

# Delay after each round of (re)transmissions. UDP is lossy so unanswered
# requests are resent, but the total is bounded so that a network which
# blocks STUN entirely fails quickly rather than hanging.
_RETRANSMIT_DELAYS = (0.5, 0.5, 1.0)

Address = tuple[str, int]


class NatMapping(enum.Enum):
    """How a NAT assigns external addresses to outbound flows."""

    NoNat = 'No NAT'
    """Host is not behind a NAT and is directly reachable."""
    EndpointIndependent = 'Endpoint-independent mapping'
    """Host is behind a NAT which reuses one external address for all peers."""
    AddressDependent = 'Address-dependent mapping'
    """Host is behind a NAT which uses a different address for each peer."""


class Result(NamedTuple):
    """Result of a NAT mapping behavior check.

    Attributes:
        mapping: Mapping behavior of the NAT this host is behind.
        external_ip: External IP of this host.
        external_port: External port of this host. This is only stable across
            peers when `mapping` is not
            [`AddressDependent`][proxystore.p2p.nat.NatMapping].
        hole_punching_likely: Whether NAT traversal is expected to work.
    """

    mapping: NatMapping
    external_ip: str
    external_port: int
    hole_punching_likely: bool


def _encode_request(transaction_id: bytes) -> bytes:
    """Encode a STUN binding request with an empty body."""
    return struct.pack(
        _HEADER_FORMAT,
        _BINDING_REQUEST,
        0,
        _MAGIC_COOKIE,
        transaction_id,
    )


def _decode_address(attribute_type: int, value: bytes) -> Address | None:
    """Decode a (XOR-)MAPPED-ADDRESS attribute value, IPv4 only."""
    if len(value) < 8 or value[1] != _IPV4_FAMILY:
        return None

    (port,) = struct.unpack('>H', value[2:4])
    packed_ip = value[4:8]

    if attribute_type == _XOR_MAPPED_ADDRESS:
        # The address and port are obfuscated with the magic cookie so that
        # NATs which rewrite payloads do not mangle them (RFC 5389 15.2).
        port ^= _MAGIC_COOKIE >> 16
        cookie = struct.pack('>I', _MAGIC_COOKIE)
        packed_ip = bytes(
            a ^ b for a, b in zip(packed_ip, cookie, strict=True)
        )

    return socket.inet_ntoa(packed_ip), port


def _decode_response(data: bytes) -> tuple[bytes, Address] | None:
    """Decode a STUN binding success response.

    Returns:
        Tuple of the transaction ID and the reflected external address, or
        `None` if the datagram is not a binding success carrying an IPv4
        address.
    """
    if len(data) < _HEADER_BYTES:
        return None

    message_type, length, cookie, transaction_id = struct.unpack(
        _HEADER_FORMAT,
        data[:_HEADER_BYTES],
    )
    if message_type != _BINDING_SUCCESS or cookie != _MAGIC_COOKIE:
        return None

    body = data[_HEADER_BYTES : _HEADER_BYTES + length]
    address: Address | None = None
    offset = 0

    while offset + 4 <= len(body):
        attribute_type, attribute_length = struct.unpack(
            '>HH',
            body[offset : offset + 4],
        )
        value = body[offset + 4 : offset + 4 + attribute_length]

        if attribute_type in (_MAPPED_ADDRESS, _XOR_MAPPED_ADDRESS):
            decoded = _decode_address(attribute_type, value)
            # XOR-MAPPED-ADDRESS is preferred when a server sends both.
            if decoded is not None and (
                address is None or attribute_type == _XOR_MAPPED_ADDRESS
            ):
                address = decoded

        # Attribute values are padded to a multiple of four bytes.
        offset += 4 + attribute_length + (-attribute_length % 4)

    return (transaction_id, address) if address is not None else None


class _StunProtocol(asyncio.DatagramProtocol):
    """Collects binding responses, keyed by transaction ID."""

    def __init__(self, expected: int) -> None:
        self.expected = expected
        self.responses: dict[bytes, Address] = {}
        self.complete = asyncio.Event()

    def datagram_received(self, data: bytes, addr: Address) -> None:
        decoded = _decode_response(data)
        if decoded is None:  # pragma: no cover
            return

        transaction_id, address = decoded
        self.responses[transaction_id] = address
        if len(self.responses) >= self.expected:
            self.complete.set()


async def _resolve_servers(
    servers: tuple[tuple[str, int], ...],
) -> list[Address]:
    """Resolve STUN server hostnames, dropping failures and duplicate IPs."""
    loop = asyncio.get_running_loop()
    results = await asyncio.gather(
        *(
            loop.getaddrinfo(host, port, family=socket.AF_INET)
            for host, port in servers
        ),
        return_exceptions=True,
    )

    resolved: list[Address] = []
    seen: set[str] = set()

    for (host, port), result in zip(servers, results, strict=True):
        if isinstance(result, BaseException) or not result:
            logger.debug(f'Failed to resolve STUN server {host}:{port}')
            continue

        ip = str(result[0][4][0])
        # Servers sharing an IP only provide one measurement point.
        if ip in seen:
            logger.debug(f'Skipping {host}:{port} which duplicates {ip}')
            continue

        seen.add(ip)
        resolved.append((ip, port))

    return resolved


def _local_address(server: Address) -> str:
    """Get the local address the kernel would use to reach a server."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        # Connecting a UDP socket sends nothing but resolves the route.
        s.connect(server)
        return s.getsockname()[0]


async def check_nat(
    source_ip: str = '0.0.0.0',
    source_port: int = 0,
    timeout: float = 2.0,
) -> Result:
    """Check the NAT mapping behavior of this host.

    Sends STUN binding requests from a single socket to several servers on
    different IP addresses. If every server reflects the same external
    address then the NAT reuses one mapping for all destinations and
    hole-punching can work. If the addresses differ then the mapping is
    address-dependent and a relay is required.

    Args:
        source_ip: Address to bind to.
        source_port: Port to bind to. The default binds an ephemeral port.
        timeout: Maximum number of seconds to wait for responses.

    Returns:
        Result describing the mapping behavior and external address.

    Raises:
        RuntimeError: if fewer than two STUN servers respond, in which case
            the mapping behavior cannot be determined.
    """
    servers = await _resolve_servers(_STUN_SERVERS)
    if len(servers) < 2:
        raise RuntimeError(
            f'Only {len(servers)} STUN servers could be resolved but at '
            'least two are needed to determine NAT mapping behavior.',
        )

    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: _StunProtocol(len(servers)),
        local_addr=(source_ip, source_port),
        family=socket.AF_INET,
    )

    requests = {secrets.token_bytes(_TRANSACTION_ID_BYTES): s for s in servers}
    deadline = loop.time() + timeout

    try:
        for delay in _RETRANSMIT_DELAYS:
            pending = [
                (transaction_id, server)
                for transaction_id, server in requests.items()
                if transaction_id not in protocol.responses
            ]
            if not pending:
                break

            for transaction_id, server in pending:
                transport.sendto(_encode_request(transaction_id), server)

            remaining = min(delay, deadline - loop.time())
            if remaining <= 0:
                break

            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(protocol.complete.wait(), remaining)
    finally:
        transport.close()

    addresses = list(protocol.responses.values())
    if len(addresses) < 2:
        raise RuntimeError(
            f'Only {len(addresses)} of {len(servers)} STUN servers responded '
            'but at least two are needed to determine NAT mapping behavior. '
            'Enable debug level logging for more details.',
        )

    external_ip, external_port = addresses[0]

    if any(address != addresses[0] for address in addresses):
        mapping = NatMapping.AddressDependent
    elif external_ip == _local_address(servers[0]):
        mapping = NatMapping.NoNat
    else:
        mapping = NatMapping.EndpointIndependent

    return Result(
        mapping=mapping,
        external_ip=external_ip,
        external_port=external_port,
        hole_punching_likely=mapping is not NatMapping.AddressDependent,
    )


async def check_nat_and_log(
    source_ip: str = '0.0.0.0',
    source_port: int = 0,
    timeout: float = 2.0,
) -> None:
    """Check the NAT mapping behavior of this host and log the results.

    Wrapper around [`check_nat()`][proxystore.p2p.nat.check_nat]
    that logs the results rather than return them.

    Args:
        source_ip: Address to bind to.
        source_port: Port to bind to. The default binds an ephemeral port.
        timeout: Maximum number of seconds to wait for responses.
    """
    logger.info('Checking NAT behavior. This may take a moment...')
    try:
        result = await check_nat(
            source_ip=source_ip,
            source_port=source_port,
            timeout=timeout,
        )
    except Exception as e:
        logger.error(f'Failed to determine NAT behavior: {e}')
        return

    logger.info(f'NAT Behavior:   {result.mapping.value}')
    logger.info(f'External IP:    {result.external_ip}')
    logger.info(f'External Port:  {result.external_port}')

    if result.hole_punching_likely:
        logger.info(
            'NAT traversal for peer-to-peer methods (e.g., hole-punching) '
            'is likely to work.',
        )
    else:
        logger.warning(
            'This NAT assigns a different external address to each peer so '
            'NAT traversal (e.g., hole-punching) will not work reliably. '
            'Peer-to-peer methods may require a relay.',
        )
