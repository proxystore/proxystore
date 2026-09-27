"""Client for communicating with a local endpoint.

Note:
    Clients communicate with endpoints on the local network over TCP using
    the protocol defined in
    [`proxystore.endpoint.protocol`][proxystore.endpoint.protocol].
    It is not intended that clients from outside the local network interact
    with an endpoint this way. (Rather, they should connect to their own
    local endpoint, which peers with remote endpoints.)
"""

from __future__ import annotations

import hmac
import logging
import os
import socket
import ssl
import warnings
from types import TracebackType
from typing import Self

from proxystore.endpoint.auth import certificate_fingerprint
from proxystore.endpoint.auth import EndpointToken
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.exceptions import EndpointAuthError
from proxystore.endpoint.exceptions import EndpointConnectionError
from proxystore.endpoint.exceptions import EndpointError
from proxystore.endpoint.exceptions import EndpointNotRunningError
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.exceptions import ObjectSizeExceededError
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.protocol import Auth
from proxystore.endpoint.protocol import Challenge
from proxystore.endpoint.protocol import EndpointInfo
from proxystore.endpoint.protocol import ExistsResult
from proxystore.endpoint.protocol import HandshakeReader
from proxystore.endpoint.protocol import Hello
from proxystore.endpoint.protocol import Message
from proxystore.endpoint.protocol import MessageReader
from proxystore.endpoint.protocol import MIN_PROTOCOL_VERSION
from proxystore.endpoint.protocol import NONCE_SIZE
from proxystore.endpoint.protocol import Op
from proxystore.endpoint.protocol import PingResult
from proxystore.endpoint.protocol import Preamble
from proxystore.endpoint.protocol import PROTOCOL_VERSION
from proxystore.endpoint.protocol import raise_for_status
from proxystore.endpoint.protocol import Request
from proxystore.endpoint.protocol import Status
from proxystore.endpoint.protocol import supports_version
from proxystore.endpoint.protocol import VERSION_DOCS_URL
from proxystore.endpoint.protocol import Versions
from proxystore.serialize import BytesLike
from proxystore.warnings import VersionMismatchWarning

logger = logging.getLogger(__name__)

# Payloads smaller than this are copied into the same buffer as the header
# so the request is sent with a single system call.
_COALESCE_THRESHOLD = 64 * 1024
_MAX_REQUEST_ID = 2**32 - 1


class EndpointClient:
    """Connection to a local endpoint.

    Use [`from_name()`][proxystore.endpoint.client.EndpointClient.from_name]
    to connect to a local endpoint by name, or
    [`connect()`][proxystore.endpoint.client.EndpointClient.connect] to
    connect to an address directly.

    Warning:
        A client is not thread-safe because a connection can only process one
        request at a time. Use a separate client per thread.

    Example:
        ```python
        with EndpointClient.from_name('my-endpoint') as client:
            client.set('key', b'value')
            assert client.get('key') == b'value'
        ```

    Args:
        sock: Connected socket that has completed the handshake.
        info: Information about the endpoint.
        protocol_version: Protocol version negotiated in the handshake.
    """

    def __init__(
        self,
        sock: socket.socket,
        info: EndpointInfo,
        *,
        protocol_version: int,
    ) -> None:
        self._socket = sock
        self.info = info
        self.protocol_version = protocol_version
        self.closed = False
        self._next_request_id = 1

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f'{type(self).__name__}(id={self.info.id}, '
            f'name={self.info.name!r})'
        )

    @classmethod
    def connect(
        cls,
        host: str,
        port: int,
        token: EndpointToken,
        *,
        tls_fingerprint: str | None = None,
        timeout: float | None = 10,
    ) -> Self:
        """Connect to an endpoint and complete the handshake.

        Args:
            host: Host address of the endpoint.
            port: Port of the endpoint.
            token: Token of the endpoint (see
                [`EndpointDir.read_connection()`][proxystore.endpoint.directory.EndpointDir.read_connection]).
            tls_fingerprint: SHA-256 fingerprint of the endpoint's TLS
                certificate (see
                [`EndpointDir.read_connection()`][proxystore.endpoint.directory.EndpointDir.read_connection]).
                If provided, the connection is encrypted with TLS and the
                endpoint's certificate must match the fingerprint.
            timeout: Timeout in seconds for connecting and completing the
                handshake. Requests after the handshake have no timeout
                because large transfers can take arbitrarily long.

        Warns:
            VersionMismatchWarning: If the endpoint uses a different
                ProxyStore version or Python minor version than this client.

        Raises:
            EndpointNotRunningError: If the connection is refused.
            EndpointConnectionError: If the connection cannot be established
                or is lost during the handshake (e.g., a timeout).
            EndpointAuthError: If the client or endpoint fails
                authentication.
            EndpointProtocolError: If the endpoint uses an incompatible
                protocol.
        """
        try:
            sock = socket.create_connection((host, port), timeout=timeout)
        except ConnectionRefusedError as e:
            raise EndpointNotRunningError(
                f'Connection to the endpoint at {host}:{port} was refused. '
                'Is the endpoint running?',
            ) from e
        except OSError as e:
            raise EndpointConnectionError(
                f'Unable to connect to the endpoint at {host}:{port}: {e}',
            ) from e

        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            if tls_fingerprint is not None:
                sock = _wrap_tls(sock, tls_fingerprint)
            info, version = _handshake(sock, token)
            sock.settimeout(None)
        except OSError as e:
            sock.close()
            raise EndpointConnectionError(
                f'Lost connection to the endpoint at {host}:{port} during '
                f'the handshake: {e}',
            ) from e
        except BaseException:
            sock.close()
            raise

        warning = Versions.current().mismatch_warning(info.versions)
        if warning is not None:
            warnings.warn(
                f'Endpoint {info.name} ({info.id.short()}) uses different '
                f'versions than this client: {warning}',
                VersionMismatchWarning,
                stacklevel=2,
            )
        logger.debug(
            'Connected to endpoint %s at %s:%s (tls=%s)',
            info.id.log_name(info.name),
            host,
            port,
            tls_fingerprint is not None,
        )
        return cls(sock, info, protocol_version=version)

    @classmethod
    def from_dir(
        cls,
        endpoint_dir: EndpointDir,
        *,
        timeout: float | None = 10,
    ) -> Self:
        """Connect to a local endpoint using its connection file.

        The connection file is read each time because the endpoint writes
        a new one each time it starts.

        Args:
            endpoint_dir: Directory of the endpoint.
            timeout: Timeout in seconds for connecting and completing the
                handshake.

        Raises:
            EndpointNotFoundError: If the endpoint directory does not exist.
            EndpointNotRunningError: If the endpoint's connection file does
                not exist (i.e., the endpoint is not running).
            EndpointAuthError: If the connection file cannot be read or is
                malformed.
            EndpointError: If the connection or handshake fails (see
                [`connect()`][proxystore.endpoint.client.EndpointClient.connect]).
        """
        endpoint_dir.check_exists()
        try:
            info = endpoint_dir.read_connection()
        except FileNotFoundError as e:
            raise EndpointNotRunningError(
                _missing_connection_file_message(endpoint_dir),
            ) from e
        except (OSError, ValueError) as e:
            raise EndpointAuthError(
                f'Unable to read the connection file of the endpoint in '
                f'{endpoint_dir}: {e}',
            ) from e
        return cls.connect(
            info.host,
            info.port,
            info.token,
            tls_fingerprint=info.tls_fingerprint,
            timeout=timeout,
        )

    @classmethod
    def from_name(
        cls,
        name: str,
        *,
        proxystore_dir: str | None = None,
        timeout: float | None = 10,
    ) -> Self:
        """Connect to a local endpoint by name.

        Args:
            name: Name of the endpoint.
            proxystore_dir: ProxyStore home directory containing the
                endpoint. Defaults to
                [`home_dir()`][proxystore.utils.environment.home_dir].
            timeout: Timeout in seconds for connecting and completing the
                handshake.

        Raises:
            EndpointNotFoundError: If no endpoint with the name exists.
            EndpointError: If connecting to the endpoint fails (see
                [`from_dir()`][proxystore.endpoint.client.EndpointClient.from_dir]).
        """
        endpoint_dir = EndpointDir.from_name(name, proxystore_dir)
        return cls.from_dir(endpoint_dir, timeout=timeout)

    def close(self) -> None:
        """Close the connection."""
        if not self.closed:
            self.closed = True
            self._socket.close()
            logger.debug(
                'Closed connection to endpoint %s',
                self.info.id.log_name(self.info.name),
            )

    def evict(self, key: str, target: str | None = None) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
            target: Optional ID of a peer endpoint to forward the operation to.

        Raises:
            ValueError: If `target` is not a valid endpoint ID.
            EndpointError: If the request fails.
        """
        self._request(Op.EVICT, _request(key, target))

    def exists(self, key: str, target: str | None = None) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.
            target: Optional ID of a peer endpoint to forward the operation to.

        Returns:
            If an object associated with the key exists.

        Raises:
            ValueError: If `target` is not a valid endpoint ID.
            EndpointError: If the request fails.
        """
        response = self._request(Op.EXISTS, _request(key, target))
        return ExistsResult.decode(response.meta).exists

    def get(
        self,
        key: str,
        target: str | None = None,
    ) -> bytearray | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with object to retrieve.
            target: Optional ID of a peer endpoint to forward the operation to.

        Returns:
            Serialized object or `None` if the object does not exist.

        Raises:
            ValueError: If `target` is not a valid endpoint ID.
            EndpointError: If the request fails.
        """
        response = self._request(Op.GET, _request(key, target))
        if response.code == Status.NOT_FOUND:
            return None
        data = response.data
        # Data is always read into a bytearray unless it is empty.
        return data if isinstance(data, bytearray) else bytearray(data)

    def set(
        self,
        key: str,
        data: BytesLike,
        target: str | None = None,
    ) -> None:
        """Set the serialized object associated with the key.

        Args:
            key: Key to associate with the object.
            data: Serialized object.
            target: Optional ID of a peer endpoint to forward the operation to.

        Raises:
            ObjectSizeExceededError: If the size of `data` exceeds the
                maximum object size of the endpoint.
            ValueError: If `target` is not a valid endpoint ID.
            EndpointError: If the request fails.
        """
        size = memoryview(data).nbytes
        max_size = self.info.max_object_size
        if max_size is not None and size > max_size:
            raise ObjectSizeExceededError(
                f'Data size ({size} bytes) exceeds the maximum object size '
                f'of the endpoint ({max_size} bytes).',
            )
        self._request(Op.SET, _request(key, target), data)

    def ping(self, target: str | None = None) -> PingResult:
        """Measure the latency of and path to a peer endpoint.

        The local endpoint sends a request to the peer and reports the time
        until it received the response and the network path of the
        connection. The first ping to a peer includes the time to establish
        the connection.

        Args:
            target: Optional ID of the peer endpoint to ping. If `None`,
                the local endpoint is pinged.

        Raises:
            ValueError: If `target` is not a valid endpoint ID.
            EndpointError: If the request fails.
        """
        response = self._request(Op.PING, _request(None, target))
        return PingResult.decode(response.meta)

    def _request(
        self,
        op: Op,
        request: Request,
        data: BytesLike | None = None,
    ) -> Message:
        if self.closed:
            raise EndpointConnectionError(
                'Connection to the endpoint is closed.',
            )

        payload = _as_bytes_view(data) if data is not None else None
        data_len = 0 if payload is None else len(payload)
        request_id = self._next_request_id
        # Request IDs are in the range [1, 2^32 - 1] because 0 is reserved
        # for handshake messages.
        self._next_request_id = request_id % _MAX_REQUEST_ID + 1
        message = Message(op, request.encode(), request_id=request_id)
        head = message.pack_head(data_len)

        try:
            if payload is None:
                self._socket.sendall(head)
            elif data_len < _COALESCE_THRESHOLD:
                self._socket.sendall(head + payload)
            else:
                self._socket.sendall(head)
                self._socket.sendall(payload)

            response = _recv_message(self._socket)
            if response.request_id != request_id:
                raise EndpointProtocolError(
                    f'Expected a response to request {request_id} but got '
                    f'a response to request {response.request_id}.',
                )
        except EndpointError:
            self.close()
            raise
        except OSError as e:
            self.close()
            raise EndpointConnectionError(
                f'Lost connection to the endpoint: {e}',
            ) from e
        except BaseException:
            # An interrupted request (e.g., KeyboardInterrupt) leaves the
            # connection in an unknown state so it cannot be reused.
            self.close()
            raise

        try:
            raise_for_status(response, op)
        except (EndpointProtocolError, ObjectSizeExceededError):
            # The connection is in an unknown state after a bad request, and
            # the endpoint may close the connection after rejecting data that
            # is too large because it did not read the data.
            self.close()
            raise
        return response


def _missing_connection_file_message(endpoint_dir: EndpointDir) -> str:
    message = (
        'Unable to find the connection file of the endpoint in '
        f'{endpoint_dir}.'
    )
    if endpoint_dir.lock().is_locked():
        # The endpoint holds its lock before it writes its connection file.
        return (
            f'{message} The endpoint is running but has not written its '
            'connection file so it is likely still starting.'
        )
    return f'{message} Is the endpoint running?'


def _request(key: str | None, target: str | None) -> Request:
    # The target is parsed first for a clear error if it is invalid.
    parsed = None if target is None else EndpointId.from_str(target)
    return Request(key=key, target=parsed)


def _as_bytes_view(data: BytesLike) -> memoryview:
    view = memoryview(data)
    if not view.c_contiguous:
        # Only contiguous buffers can be sent without copying.
        view = memoryview(view.tobytes())
    return view.cast('B')


def _wrap_tls(sock: socket.socket, fingerprint: str) -> ssl.SSLSocket:
    # The endpoint's certificate is self-signed so it cannot be verified
    # against a certificate authority. Instead, the certificate is pinned:
    # it must match the fingerprint in the endpoint's connection file.
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    try:
        tls_sock = context.wrap_socket(sock)
    except (ssl.SSLError, ConnectionError) as e:
        raise EndpointProtocolError(
            f'TLS handshake with the endpoint failed: {e}. Check that TLS is '
            'enabled on the endpoint.',
        ) from e

    der = tls_sock.getpeercert(binary_form=True)
    actual = certificate_fingerprint(der) if der is not None else ''
    if not hmac.compare_digest(actual, fingerprint):
        tls_sock.close()
        raise EndpointAuthError(
            'The TLS certificate of the endpoint does not match the '
            'fingerprint in the connection file. Another process may be '
            'listening on the address of the endpoint, or the endpoint was '
            'restarted since the connection file was read.',
        )
    return tls_sock


def _handshake(
    sock: socket.socket,
    token: EndpointToken,
) -> tuple[EndpointInfo, int]:
    # Returns the information of the endpoint and the negotiated version.
    hello = Hello(nonce=os.urandom(NONCE_SIZE), versions=Versions.current())
    sock.sendall(
        Preamble().pack() + Message(Op.HELLO, hello.encode()).pack_head()
    )

    preamble = _recv_exactly(sock, Preamble.SIZE)
    if preamble.startswith(b'HTTP/'):
        raise EndpointProtocolError(
            'The endpoint responded with HTTP, so it is likely running a '
            'version of ProxyStore older than the client that uses the HTTP '
            'API. Restart the endpoint with the same version of ProxyStore as '
            f'the client. See {VERSION_DOCS_URL} for details.',
        )
    version = Preamble.unpack(preamble).version
    if not supports_version(version):
        # The endpoint supports no common version so nothing after the
        # preamble can be parsed.
        raise EndpointProtocolError(
            f'Endpoint uses protocol version {version} but the client '
            f'supports protocol versions {MIN_PROTOCOL_VERSION} to '
            f'{PROTOCOL_VERSION}. Use compatible versions of ProxyStore for '
            f'the client and endpoint. See {VERSION_DOCS_URL} for details.',
        )

    challenge = Challenge.decode(_recv_handshake_message(sock))
    if not token.verify(
        'server',
        challenge.nonce,
        hello.nonce,
        challenge.proof,
    ):
        raise EndpointAuthError(
            'The endpoint failed to prove that it knows the endpoint token. '
            'Another process may be listening on the address of the '
            'endpoint, or the endpoint was restarted since the connection '
            'file was read.',
        )

    proof = token.proof('client', hello.nonce, challenge.nonce)
    sock.sendall(Message(Op.AUTH, Auth(proof=proof).encode()).pack_head())

    return EndpointInfo.decode(_recv_handshake_message(sock)), version


def _recv_handshake_message(sock: socket.socket) -> bytes:
    message = _recv_message(sock, HandshakeReader())
    if message.code == Status.UNAUTHORIZED:
        raise EndpointAuthError(
            'The endpoint rejected the token of the client. The endpoint may '
            'have been restarted since the connection file was read.',
        )
    if message.code != Status.OK:
        raise EndpointProtocolError(
            f'Endpoint returned status {message.code} during the handshake: '
            f'{message.error_message}',
        )
    return message.meta


def _recv_message(
    sock: socket.socket,
    reader: MessageReader | None = None,
) -> Message:
    reader = MessageReader() if reader is None else reader
    while not reader.done:
        reader.feed(_recv_exactly(sock, reader.size))
    return reader.message


def _recv_exactly(sock: socket.socket, size: int) -> bytearray:
    buffer = bytearray(size)
    view = memoryview(buffer)
    received = 0
    while received < size:
        n = sock.recv_into(view[received:])
        if n == 0:
            raise EndpointConnectionError(
                'The endpoint closed the connection unexpectedly.',
            )
        received += n
    return buffer
