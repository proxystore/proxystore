"""Wire protocol of endpoints.

Warning:
    This module is an internal implementation detail. Its interface may
    change between releases without notice (see
    [`proxystore.endpoint`][proxystore.endpoint]).

Clients communicate with their local endpoint over a TCP connection using
length-prefixed binary messages, and endpoints send the same messages to
their peers over iroh streams. This module only contains the encoding and
decoding logic. The
[`MessageReader`][proxystore.endpoint.protocol.MessageReader]
parses messages without performing I/O so the same parser is used by the
blocking client, the asyncio server, and the iroh streams of peers.

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
   information (ID, name, and versions).

The preamble format must never change so that clients and endpoints using
different protocol versions can detect the mismatch. If the protocol versions
differ, the endpoint replies with only its preamble and closes the connection.

After the handshake, each request and response is a message consisting of a
fixed-size [`Header`][proxystore.endpoint.protocol.Header], JSON-encoded
metadata (e.g., the key), and a raw data payload. The endpoint echoes the
request ID of each request in its response.

Peers do not use the handshake because iroh authenticates each endpoint.
Each request to a peer is sent on its own stream so the request ID is
unused. The version of the peer protocol is part of the
[`ALPN`][proxystore.endpoint.protocol.ALPN] and always matches
[`PROTOCOL_VERSION`][proxystore.endpoint.protocol.PROTOCOL_VERSION] because
peers exchange the same messages.
"""

from __future__ import annotations

import dataclasses
import enum
import json
import platform
import struct
from typing import Any
from typing import ClassVar
from typing import NamedTuple
from typing import Self

import proxystore
from proxystore.endpoint.exceptions import EndpointAuthError
from proxystore.endpoint.exceptions import EndpointError
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.exceptions import EndpointRequestError
from proxystore.endpoint.exceptions import ObjectSizeExceededError
from proxystore.endpoint.exceptions import PeeringDisabledError
from proxystore.endpoint.exceptions import PeerNotAllowedError
from proxystore.endpoint.exceptions import PeerUnavailableError
from proxystore.endpoint.identity import EndpointId
from proxystore.serialize import BytesLike

MAGIC = b'PSEP'
"""Bytes that start every connection."""
PROTOCOL_VERSION = 1
"""Version of the protocol.

Increment on any incompatible change to the handshake or message formats.
"""
ALPN = f'proxystore/{PROTOCOL_VERSION}'.encode()
"""Application protocol negotiated on connections between endpoints."""
NONCE_SIZE = 32
"""Size in bytes of the random nonces exchanged in the handshake."""
MAX_META_SIZE = 64 * 1024
"""Maximum size in bytes of the metadata in a message."""
VERSION_DOCS_URL = 'https://docs.proxystore.dev/latest/guides/endpoints/#version-compatibility'
"""Documentation on version compatibility between clients and endpoints."""


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
    PING = 7
    """Measure the latency of and path to an endpoint."""


class Status(enum.IntEnum):
    """Status codes of messages sent by an endpoint.

    The metadata of a response with an error status contains the error
    message. Each error status corresponds to one exception type (see
    [`raise_for_status()`][proxystore.endpoint.protocol.raise_for_status]).
    """

    OK = 0
    """Request succeeded."""
    NOT_FOUND = 1
    """No object is associated with the key."""
    ERROR = 2
    """Request failed for an unexpected reason."""
    UNAUTHORIZED = 3
    """Client failed authentication."""
    BAD_REQUEST = 4
    """Request was malformed."""
    TOO_LARGE = 5
    """Request data exceeds the maximum object size of the endpoint."""
    PEERING_DISABLED = 6
    """Request targets a peer but peering is disabled on the endpoint."""
    PEER_NOT_ALLOWED = 7
    """Request targets a peer which is not allowed to communicate."""
    PEER_UNAVAILABLE = 8
    """Request targets a peer which cannot be reached."""


STATUS_ERRORS: dict[Status, type[EndpointError]] = {
    Status.ERROR: EndpointRequestError,
    Status.UNAUTHORIZED: EndpointAuthError,
    Status.BAD_REQUEST: EndpointProtocolError,
    Status.TOO_LARGE: ObjectSizeExceededError,
    Status.PEERING_DISABLED: PeeringDisabledError,
    Status.PEER_NOT_ALLOWED: PeerNotAllowedError,
    Status.PEER_UNAVAILABLE: PeerUnavailableError,
}
"""Exception type corresponding to each error status."""

_ERROR_STATUSES = {error: status for status, error in STATUS_ERRORS.items()}


@dataclasses.dataclass(frozen=True, slots=True)
class Preamble:
    """Preamble that starts every connection.

    The format of the preamble must never change so that clients and
    endpoints using different protocol versions can detect the mismatch.

    Attributes:
        version: Protocol version of the sender.
    """

    FORMAT: ClassVar[struct.Struct] = struct.Struct('!4sH')
    """Format of the magic bytes and protocol version."""
    SIZE: ClassVar[int] = FORMAT.size
    """Size in bytes of the preamble."""

    version: int = PROTOCOL_VERSION

    def pack(self) -> bytes:
        """Pack the preamble."""
        return self.FORMAT.pack(MAGIC, self.version)

    @classmethod
    def unpack(cls, buffer: BytesLike) -> Self:
        """Unpack a preamble.

        Raises:
            EndpointProtocolError: If the preamble does not start with
                [`MAGIC`][proxystore.endpoint.protocol.MAGIC].
        """
        magic, version = cls.FORMAT.unpack(buffer)
        if magic != MAGIC:
            raise EndpointProtocolError(
                f'Expected connection to start with {MAGIC!r} but got '
                f'{magic!r}.',
            )
        return cls(version)


@dataclasses.dataclass(frozen=True, slots=True)
class Header:
    """Message header.

    Attributes:
        code: [`Op`][proxystore.endpoint.protocol.Op] of a request or
            [`Status`][proxystore.endpoint.protocol.Status] of a response.
        flags: Reserved for future use.
        request_id: ID chosen by the client for a request and echoed in the
            response. IDs allow requests to be pipelined in the future.
            Handshake messages and messages between peers use `0`.
        meta_len: Length in bytes of the metadata.
        data_len: Length in bytes of the data.
    """

    FORMAT: ClassVar[struct.Struct] = struct.Struct('!BBIIQ')
    """Format of the code, flags, request ID, metadata length, and data
    length."""
    SIZE: ClassVar[int] = FORMAT.size
    """Size in bytes of the header."""

    code: int
    flags: int
    request_id: int
    meta_len: int
    data_len: int

    def pack(self) -> bytes:
        """Pack the header."""
        return self.FORMAT.pack(
            self.code,
            self.flags,
            self.request_id,
            self.meta_len,
            self.data_len,
        )

    @classmethod
    def unpack(cls, buffer: BytesLike) -> Self:
        """Unpack a header.

        Raises:
            EndpointProtocolError: If the metadata length exceeds
                [`MAX_META_SIZE`][proxystore.endpoint.protocol.MAX_META_SIZE].
        """
        header = cls(*cls.FORMAT.unpack(buffer))
        if header.meta_len > MAX_META_SIZE:
            raise EndpointProtocolError(
                f'Message metadata length ({header.meta_len} bytes) exceeds '
                f'the maximum of {MAX_META_SIZE} bytes.',
            )
        return header


class Versions(NamedTuple):
    """ProxyStore and Python versions of a client or endpoint.

    Attributes:
        proxystore: ProxyStore version.
        python: Python version.
    """

    proxystore: str
    python: str

    @classmethod
    def current(cls) -> Self:
        """Get the ProxyStore and Python versions of this process."""
        return cls(proxystore.__version__, platform.python_version())

    def mismatches(self, endpoint: Versions) -> list[str]:
        """Find differences between this client's and the endpoint's versions.

        The ProxyStore versions must match exactly. The Python versions must
        have the same major and minor version because objects pickled by one
        Python version may not unpickle with another, but patch releases are
        compatible.

        Args:
            endpoint: Versions of the endpoint.

        Returns:
            Human-readable descriptions of each mismatch. Empty if the \
            versions are compatible.
        """
        mismatches = []
        if self.proxystore != endpoint.proxystore:
            mismatches.append(
                f'ProxyStore {self.proxystore} (client) vs. '
                f'{endpoint.proxystore} (endpoint)',
            )
        if self.python.split('.')[:2] != endpoint.python.split('.')[:2]:
            mismatches.append(
                f'Python {self.python} (client) vs. {endpoint.python} '
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
        id: ID of the endpoint.
        name: Name of the endpoint.
        versions: Versions of the endpoint.
        max_object_size: Maximum size in bytes of objects that can be set
            on the endpoint or `None` if there is no limit.
    """

    id: EndpointId
    name: str
    versions: Versions
    max_object_size: int | None

    def to_meta(self) -> dict[str, Any]:
        """Encode as message metadata."""
        return {
            'id': self.id,
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
        return cls(
            id=_parse_id(_get(meta, 'id', str, cls), 'id', cls),
            name=_get(meta, 'name', str, cls),
            versions=_get_versions(meta, cls),
            max_object_size=_get(
                meta,
                'max_object_size',
                (int, type(None)),
                cls,
            ),
        )


@dataclasses.dataclass(frozen=True)
class Request:
    """Metadata of a request.

    Requests are sent by clients to their local endpoint and by endpoints to
    their peers.

    Attributes:
        key: Key of the object, or `None` for operations that do not operate
            on an object (e.g.,
            [`PING`][proxystore.endpoint.protocol.Op.PING]).
        target: ID of the endpoint to forward the request to or `None`
            for the endpoint receiving the request.
    """

    key: str | None = None
    target: EndpointId | None = None

    def to_meta(self) -> dict[str, Any]:
        """Encode as message metadata."""
        return {'key': self.key, 'target': self.target}

    @classmethod
    def from_meta(cls, meta: dict[str, Any]) -> Self:
        """Decode from message metadata.

        Raises:
            EndpointProtocolError: If the metadata is malformed.
        """
        key = _get(meta, 'key', (str, type(None)), cls)
        if key is not None and len(key) == 0:
            raise _malformed(cls, 'key')
        target = _get(meta, 'target', (str, type(None)), cls)
        return cls(
            key=key,
            target=None
            if target is None
            else _parse_id(target, 'target', cls),
        )


@dataclasses.dataclass(frozen=True)
class Message:
    """A request or response message.

    Attributes:
        code: [`Op`][proxystore.endpoint.protocol.Op] of a request or
            [`Status`][proxystore.endpoint.protocol.Status] of a response.
        meta: Metadata of the message.
        data: Data of the message.
        request_id: ID of the request (see
            [`Header`][proxystore.endpoint.protocol.Header]).
    """

    code: int
    meta: dict[str, Any] = dataclasses.field(default_factory=dict)
    data: bytes | bytearray = b''
    request_id: int = 0

    @classmethod
    def error(cls, status: Status, message: str) -> Message:
        """Create a response with an error message."""
        return cls(status, {'error': message})

    @classmethod
    def from_error(cls, error: BaseException) -> Message:
        """Create a response for a request which failed with an error.

        The status is determined by
        [`error_status()`][proxystore.endpoint.protocol.error_status].
        """
        return cls.error(error_status(error), str(error))

    def pack_head(self) -> bytes:
        """Pack the header and metadata of the message.

        The data is not included so it can be sent separately without being
        copied (see
        [`pack_message()`][proxystore.endpoint.protocol.pack_message]).
        """
        return pack_message(
            self.code,
            self.meta,
            len(self.data),
            request_id=self.request_id,
        )


def error_status(error: BaseException) -> Status:
    """Get the status of a response to a request which failed with an error.

    Args:
        error: Error raised while handling the request.

    Returns:
        The status of the most specific type of `error` in \
        [`STATUS_ERRORS`][proxystore.endpoint.protocol.STATUS_ERRORS] or \
        [`Status.ERROR`][proxystore.endpoint.protocol.Status.ERROR] if there \
        is none.
    """
    for kind in type(error).__mro__:
        if kind in _ERROR_STATUSES:
            return _ERROR_STATUSES[kind]
    return Status.ERROR


def raise_for_status(
    response: Message,
    op: Op,
    *,
    source: str = 'Endpoint',
) -> Status:
    """Raise the exception corresponding to the status of a response.

    Args:
        response: Response message.
        op: Operation of the request.
        source: Description of the sender of the response for error
            messages.

    Returns:
        [`Status.OK`][proxystore.endpoint.protocol.Status.OK] or \
        [`Status.NOT_FOUND`][proxystore.endpoint.protocol.Status.NOT_FOUND].

    Raises:
        EndpointProtocolError: If the status is unknown.
        EndpointError: The type in
            [`STATUS_ERRORS`][proxystore.endpoint.protocol.STATUS_ERRORS]
            for any other status.
    """
    try:
        status = Status(response.code)
    except ValueError:
        raise EndpointProtocolError(
            f'{source} returned unknown status code {response.code}.',
        ) from None
    if status in (Status.OK, Status.NOT_FOUND):
        return status
    reason = response.meta.get('error', 'no error message provided')
    raise STATUS_ERRORS[status](
        f'{source} returned {status.name} for {op.name} request: {reason}',
    )


def exists_from_meta(meta: dict[str, Any]) -> bool:
    """Get the result of an EXISTS response from its metadata.

    Raises:
        EndpointProtocolError: If the metadata is malformed.
    """
    exists = meta.get('exists')
    if not isinstance(exists, bool):
        raise EndpointProtocolError(
            "Malformed EXISTS response: missing or invalid 'exists' field.",
        )
    return exists


@dataclasses.dataclass(frozen=True)
class PingResult:
    """Result of a [`PING`][proxystore.endpoint.protocol.Op.PING] request.

    Attributes:
        peer_rtt_ms: Time in milliseconds for the local endpoint to send a
            request to the peer and receive the response or `None` if the
            local endpoint was pinged. The time of the first ping to a peer
            includes establishing the connection.
        relayed: If the connection to the peer is relayed, or `None` if the
            path is unknown or the local endpoint was pinged.
        remote_addr: Address of the peer (or relay) on the path.
        path_rtt_ms: Round-trip time in milliseconds of the path estimated
            by QUIC.
    """

    peer_rtt_ms: float | None = None
    relayed: bool | None = None
    remote_addr: str | None = None
    path_rtt_ms: int | None = None

    def to_meta(self) -> dict[str, Any]:
        """Encode as message metadata."""
        return dataclasses.asdict(self)

    @classmethod
    def from_meta(cls, meta: dict[str, Any]) -> Self:
        """Decode from message metadata.

        Raises:
            EndpointProtocolError: If the metadata is malformed.
        """
        none = type(None)
        return cls(
            peer_rtt_ms=_get(meta, 'peer_rtt_ms', (int, float, none), cls),
            relayed=_get(meta, 'relayed', (bool, none), cls),
            remote_addr=_get(meta, 'remote_addr', (str, none), cls),
            path_rtt_ms=_get(meta, 'path_rtt_ms', (int, none), cls),
        )


def _parse_id(value: str, field: str, message: type) -> EndpointId:
    try:
        return EndpointId.from_str(value)
    except ValueError:
        raise _malformed(message, field) from None


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


def pack_message(
    code: int,
    meta: dict[str, Any] | None = None,
    data_len: int = 0,
    *,
    request_id: int = 0,
) -> bytes:
    """Pack the header and metadata of a message.

    The data (if any) is not included so it can be sent separately without
    being copied.

    Args:
        code: Op or status code.
        meta: Metadata to include in the message.
        data_len: Length in bytes of the data that will follow.
        request_id: ID of the request (see
            [`Header`][proxystore.endpoint.protocol.Header]).
    """
    meta_bytes = b'' if meta is None else encode_meta(meta)
    header = Header(code, 0, request_id, len(meta_bytes), data_len)
    return header.pack() + meta_bytes


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


class MessageReader:
    """Parser of a message which does not perform I/O.

    The caller reads exactly
    [`size`][proxystore.endpoint.protocol.MessageReader.size]
    bytes from the connection and passes them to
    [`feed()`][proxystore.endpoint.protocol.MessageReader.feed] until the
    message is [`done`][proxystore.endpoint.protocol.MessageReader.done].

    Example:
        ```python
        reader = MessageReader(max_data_size=1000)
        while not reader.done:
            reader.feed(await connection.readexactly(reader.size))
        message = reader.message
        ```

    Args:
        max_data_size: Maximum size in bytes of the data of the message or
            `None` for no limit. The size is checked once the header and
            metadata are read, before the data is read.
    """

    def __init__(self, *, max_data_size: int | None = None) -> None:
        self._max_data_size = max_data_size
        self._header: Header | None = None
        self._meta: dict[str, Any] | None = None
        self._message: Message | None = None
        self._size = Header.SIZE

    @property
    def done(self) -> bool:
        """The message has been read."""
        return self._message is not None

    @property
    def size(self) -> int:
        """Size in bytes of the next part of the message to feed.

        This is always greater than zero until the message is done.
        """
        return self._size

    @property
    def header(self) -> Header:
        """Header of the message.

        Raises:
            RuntimeError: If the header has not been read.
        """
        if self._header is None:
            raise RuntimeError('The header has not been read.')
        return self._header

    @property
    def message(self) -> Message:
        """The message.

        Raises:
            RuntimeError: If the message is not done.
        """
        if self._message is None:
            raise RuntimeError('The message has not been read.')
        return self._message

    def feed(self, buffer: bytes | bytearray) -> None:
        """Feed the next part of the message.

        Args:
            buffer: Exactly
                [`size`][proxystore.endpoint.protocol.MessageReader.size]
                bytes read from the connection.

        Raises:
            RuntimeError: If the message is already done.
            ValueError: If `buffer` is not the expected size.
            EndpointProtocolError: If the header or metadata is malformed.
            ObjectSizeExceededError: If the size of the data exceeds the
                maximum. The
                [`header`][proxystore.endpoint.protocol.MessageReader.header]
                can be used to respond to the message.
        """
        if self.done:
            raise RuntimeError('The message has already been read.')
        if len(buffer) != self._size:
            raise ValueError(
                f'Expected {self._size} bytes but got {len(buffer)} bytes.',
            )

        if self._header is None:
            self._header = Header.unpack(buffer)
            if self._header.meta_len > 0:
                self._size = self._header.meta_len
                return
            buffer = b''
        if self._meta is None:
            self._meta = decode_meta(buffer)
            self._read_meta()
            return
        self._message = Message(
            self._header.code,
            self._meta,
            buffer,
            self._header.request_id,
        )
        self._size = 0

    def _read_meta(self) -> None:
        assert self._header is not None
        assert self._meta is not None
        data_len = self._header.data_len
        if self._max_data_size is not None and data_len > self._max_data_size:
            raise ObjectSizeExceededError(
                f'Data size ({data_len} bytes) exceeds the maximum object '
                f'size of the endpoint ({self._max_data_size} bytes).',
            )
        if data_len > 0:
            self._size = data_len
        else:
            self._message = Message(
                self._header.code,
                self._meta,
                request_id=self._header.request_id,
            )
            self._size = 0
