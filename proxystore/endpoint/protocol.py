"""Client-endpoint wire protocol.

Clients communicate with their local endpoint over a TCP connection using
length-prefixed binary messages. This module only contains the encoding and
decoding logic, and it only depends on the standard library so the client
does not require any of the `endpoints` extra dependencies.

A connection starts with a handshake:

1. The client sends a preamble ([`MAGIC`][proxystore.endpoint.protocol.MAGIC]
   and [`PROTOCOL_VERSION`][proxystore.endpoint.protocol.PROTOCOL_VERSION])
   followed by a [`HELLO`][proxystore.endpoint.protocol.Op.HELLO] message
   with a random nonce and the client's versions.
2. The endpoint replies with its preamble and a message with its own nonce and
   a proof that it knows the endpoint's token.
3. The client verifies the endpoint's proof then sends an
   [`AUTH`][proxystore.endpoint.protocol.Op.AUTH] message with its own proof.
4. The endpoint verifies the client's proof and replies with its
   information (UUID, name, and versions).

The preamble format must never change so that clients and endpoints using
different protocol versions can detect the mismatch. If the protocol versions
differ, the endpoint replies with only its preamble and closes the connection.

After the handshake, each request and response is a message consisting of a
fixed-size header, JSON-encoded metadata (e.g., the key), and a raw data
payload.
"""

from __future__ import annotations

import dataclasses
import enum
import json
import platform
import struct
import uuid
from typing import Any
from typing import NamedTuple
from typing import Self

import proxystore
from proxystore.endpoint.exceptions import EndpointProtocolError

MAGIC = b'PSEP'
"""Bytes that start every connection."""
PROTOCOL_VERSION = 1
"""Version of the protocol.

Increment on any incompatible change to the handshake or message formats.
"""
NONCE_SIZE = 32
"""Size in bytes of the random nonces exchanged in the handshake."""
MAX_META_SIZE = 64 * 1024
"""Maximum size in bytes of the metadata in a message."""
HTTP_METHODS = (b'GET ', b'POST', b'HEAD', b'PUT ')
"""Leading bytes of HTTP requests sent by clients using the old HTTP API."""
VERSION_DOCS_URL = 'https://docs.proxystore.dev/latest/guides/endpoints/#version-compatibility'
"""Documentation on version compatibility between clients and endpoints."""

PREAMBLE = struct.Struct('!4sH')
"""Preamble format: magic and protocol version."""
HEADER = struct.Struct('!BBIQ')
"""Message header format: op/status, flags, metadata length, data length."""


class Op(enum.IntEnum):
    """Operation codes of messages sent by a client."""

    HELLO = 1
    """First message of the handshake."""
    AUTH = 2
    """Second message of the handshake with the client's proof."""
    GET = 3
    """Get the object associated with a key."""
    SET = 4
    """Set the object associated with a key."""
    EXISTS = 5
    """Check if an object associated with a key exists."""
    EVICT = 6
    """Evict the object associated with a key."""


class Status(enum.IntEnum):
    """Status codes of messages sent by an endpoint."""

    OK = 0
    """Request succeeded."""
    NOT_FOUND = 1
    """No object is associated with the key."""
    ERROR = 2
    """Request failed. The metadata contains the error message."""
    UNAUTHORIZED = 3
    """Client failed authentication."""
    BAD_REQUEST = 4
    """Request was malformed."""
    TOO_LARGE = 5
    """Request data exceeds the maximum object size of the endpoint."""


class Header(NamedTuple):
    """Message header.

    Attributes:
        code: [`Op`][proxystore.endpoint.protocol.Op] of a request or
            [`Status`][proxystore.endpoint.protocol.Status] of a response.
        flags: Reserved for future use.
        meta_len: Length in bytes of the metadata.
        data_len: Length in bytes of the data.
    """

    code: int
    flags: int
    meta_len: int
    data_len: int


class Versions(NamedTuple):
    """ProxyStore and Python versions of a client or endpoint.

    Attributes:
        proxystore: ProxyStore version.
        python: Python version.
    """

    proxystore: str
    python: str


def local_versions() -> Versions:
    """Get the ProxyStore and Python versions of this process."""
    return Versions(proxystore.__version__, platform.python_version())


def version_mismatches(client: Versions, endpoint: Versions) -> list[str]:
    """Find version differences between a client and endpoint.

    The ProxyStore versions must match exactly. The Python versions must
    have the same major and minor version because objects pickled by one
    Python version may not unpickle with another, but patch releases are
    compatible.

    Args:
        client: Versions of the client.
        endpoint: Versions of the endpoint.

    Returns:
        Human-readable descriptions of each mismatch. Empty if the versions \
        are compatible.
    """
    mismatches = []
    if client.proxystore != endpoint.proxystore:
        mismatches.append(
            f'ProxyStore {client.proxystore} (client) vs. '
            f'{endpoint.proxystore} (endpoint)',
        )
    if client.python.split('.')[:2] != endpoint.python.split('.')[:2]:
        mismatches.append(
            f'Python {client.python} (client) vs. {endpoint.python} '
            '(endpoint)',
        )
    return mismatches


@dataclasses.dataclass(frozen=True)
class Hello:
    """First message of the handshake sent by the client.

    Attributes:
        nonce: Random nonce chosen by the client.
        versions: Versions of the client.
    """

    nonce: bytes
    versions: Versions

    def to_meta(self) -> dict[str, Any]:
        """Encode as message metadata."""
        return {'nonce': self.nonce.hex(), **self.versions._asdict()}

    @classmethod
    def from_meta(cls, meta: dict[str, Any]) -> Self:
        """Decode from message metadata.

        Raises:
            EndpointProtocolError: If the metadata is malformed.
        """
        return cls(
            nonce=_get_hex(meta, 'nonce', cls, size=NONCE_SIZE),
            versions=_get_versions(meta, cls),
        )


@dataclasses.dataclass(frozen=True)
class Challenge:
    """Reply of the endpoint to [`Hello`][proxystore.endpoint.protocol.Hello].

    Attributes:
        nonce: Random nonce chosen by the endpoint.
        proof: Proof that the endpoint knows the token.
    """

    nonce: bytes
    proof: bytes

    def to_meta(self) -> dict[str, Any]:
        """Encode as message metadata."""
        return {'nonce': self.nonce.hex(), 'proof': self.proof.hex()}

    @classmethod
    def from_meta(cls, meta: dict[str, Any]) -> Self:
        """Decode from message metadata.

        Raises:
            EndpointProtocolError: If the metadata is malformed.
        """
        return cls(
            nonce=_get_hex(meta, 'nonce', cls, size=NONCE_SIZE),
            proof=_get_hex(meta, 'proof', cls),
        )


@dataclasses.dataclass(frozen=True)
class Auth:
    """Second message of the handshake with the proof of the client.

    Attributes:
        proof: Proof that the client knows the token.
    """

    proof: bytes

    def to_meta(self) -> dict[str, Any]:
        """Encode as message metadata."""
        return {'proof': self.proof.hex()}

    @classmethod
    def from_meta(cls, meta: dict[str, Any]) -> Self:
        """Decode from message metadata.

        Raises:
            EndpointProtocolError: If the metadata is malformed.
        """
        return cls(proof=_get_hex(meta, 'proof', cls))


@dataclasses.dataclass(frozen=True)
class EndpointInfo:
    """Information about an endpoint sent at the end of the handshake.

    Attributes:
        uuid: UUID of the endpoint.
        name: Name of the endpoint.
        versions: Versions of the endpoint.
        max_object_size: Maximum size in bytes of objects that can be set
            on the endpoint or `None` if there is no limit.
    """

    uuid: uuid.UUID
    name: str
    versions: Versions
    max_object_size: int | None

    def to_meta(self) -> dict[str, Any]:
        """Encode as message metadata."""
        return {
            'uuid': str(self.uuid),
            'name': self.name,
            'max_object_size': self.max_object_size,
            **self.versions._asdict(),
        }

    @classmethod
    def from_meta(cls, meta: dict[str, Any]) -> Self:
        """Decode from message metadata.

        Raises:
            EndpointProtocolError: If the metadata is malformed.
        """
        uuid_str = _get(meta, 'uuid', str, cls)
        try:
            endpoint_uuid = uuid.UUID(uuid_str)
        except ValueError:
            raise _malformed(cls, 'uuid') from None
        return cls(
            uuid=endpoint_uuid,
            name=_get(meta, 'name', str, cls),
            versions=_get_versions(meta, cls),
            max_object_size=_get(
                meta,
                'max_object_size',
                (int, type(None)),
                cls,
            ),
        )


def _malformed(message: type, field: str) -> EndpointProtocolError:
    return EndpointProtocolError(
        f'Malformed {message.__name__} message: missing or invalid '
        f'{field!r} field.',
    )


def _get(
    meta: dict[str, Any],
    field: str,
    kind: type | tuple[type, ...],
    message: type,
) -> Any:
    if field not in meta or not isinstance(meta[field], kind):
        raise _malformed(message, field)
    return meta[field]


def _get_hex(
    meta: dict[str, Any],
    field: str,
    message: type,
    *,
    size: int | None = None,
) -> bytes:
    try:
        value = bytes.fromhex(_get(meta, field, str, message))
    except ValueError:
        raise _malformed(message, field) from None
    if size is not None and len(value) != size:
        raise _malformed(message, field)
    return value


def _get_versions(meta: dict[str, Any], message: type) -> Versions:
    return Versions(
        proxystore=_get(meta, 'proxystore', str, message),
        python=_get(meta, 'python', str, message),
    )


def pack_preamble(version: int = PROTOCOL_VERSION) -> bytes:
    """Pack the connection preamble."""
    return PREAMBLE.pack(MAGIC, version)


def unpack_preamble(buffer: bytes | bytearray) -> int:
    """Unpack the connection preamble.

    Returns:
        Protocol version of the peer.

    Raises:
        EndpointProtocolError: If the preamble does not start with
            [`MAGIC`][proxystore.endpoint.protocol.MAGIC].
    """
    magic, version = PREAMBLE.unpack(buffer)
    if magic != MAGIC:
        raise EndpointProtocolError(
            f'Expected connection to start with {MAGIC!r} but got {magic!r}.',
        )
    return version


def pack_message(
    code: int,
    meta: dict[str, Any] | None = None,
    data_len: int = 0,
) -> bytes:
    """Pack the header and metadata of a message.

    The data (if any) is not included so it can be sent separately without
    being copied.

    Args:
        code: Op or status code.
        meta: Metadata to include in the message.
        data_len: Length in bytes of the data that will follow.
    """
    meta_bytes = b'' if meta is None else encode_meta(meta)
    return HEADER.pack(code, 0, len(meta_bytes), data_len) + meta_bytes


def unpack_header(buffer: bytes | bytearray) -> Header:
    """Unpack a message header.

    Raises:
        EndpointProtocolError: If the metadata length exceeds
            [`MAX_META_SIZE`][proxystore.endpoint.protocol.MAX_META_SIZE].
    """
    header = Header(*HEADER.unpack(buffer))
    if header.meta_len > MAX_META_SIZE:
        raise EndpointProtocolError(
            f'Message metadata length ({header.meta_len} bytes) exceeds the '
            f'maximum of {MAX_META_SIZE} bytes.',
        )
    return header


def encode_meta(meta: dict[str, Any]) -> bytes:
    """Encode message metadata."""
    return json.dumps(meta, separators=(',', ':')).encode()


def decode_meta(buffer: bytes | bytearray) -> dict[str, Any]:
    """Decode message metadata.

    Raises:
        EndpointProtocolError: If the metadata is not a JSON object.
    """
    if len(buffer) == 0:
        return {}
    try:
        meta = json.loads(buffer)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise EndpointProtocolError(
            f'Failed to decode message metadata: {e}',
        ) from e
    if not isinstance(meta, dict):
        raise EndpointProtocolError(
            f'Expected message metadata to be a JSON object but got '
            f'{type(meta).__name__}.',
        )
    return meta
