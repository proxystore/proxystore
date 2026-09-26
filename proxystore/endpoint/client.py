"""Client for communicating with a local endpoint.

Note:
    Clients communicate with endpoints on the local network over TCP using
    the protocol defined in
    [`proxystore.endpoint.protocol`][proxystore.endpoint.protocol].
    It is not intended that clients from outside the local network interact
    with an endpoint this way. (Rather, they should connect to their own
    local endpoint, which peers with remote endpoints.)

This module does not depend on the `endpoints` extra dependencies so clients
do not need to install them.
"""

from __future__ import annotations

import hmac
import os
import socket
import ssl
import uuid
import warnings
from types import TracebackType
from typing import Any
from typing import Self

from proxystore.endpoint.auth import certificate_fingerprint
from proxystore.endpoint.auth import compute_proof
from proxystore.endpoint.auth import verify_proof
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.exceptions import EndpointAuthError
from proxystore.endpoint.exceptions import EndpointConnectionError
from proxystore.endpoint.exceptions import EndpointError
from proxystore.endpoint.exceptions import EndpointNotFoundError
from proxystore.endpoint.exceptions import EndpointNotRunningError
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.exceptions import EndpointRequestError
from proxystore.endpoint.exceptions import ObjectSizeExceededError
from proxystore.endpoint.protocol import Auth
from proxystore.endpoint.protocol import Challenge
from proxystore.endpoint.protocol import decode_meta
from proxystore.endpoint.protocol import EndpointInfo
from proxystore.endpoint.protocol import Header
from proxystore.endpoint.protocol import Hello
from proxystore.endpoint.protocol import NONCE_SIZE
from proxystore.endpoint.protocol import Op
from proxystore.endpoint.protocol import pack_message
from proxystore.endpoint.protocol import Preamble
from proxystore.endpoint.protocol import PROTOCOL_VERSION
from proxystore.endpoint.protocol import Request
from proxystore.endpoint.protocol import Status
from proxystore.endpoint.protocol import VERSION_DOCS_URL
from proxystore.endpoint.protocol import Versions
from proxystore.endpoint.warnings import EndpointVersionWarning
from proxystore.serialize import BytesLike
from proxystore.utils.environment import home_dir

# Payloads smaller than this are copied into the same buffer as the header
# so the request is sent with a single system call.
_COALESCE_THRESHOLD = 64 * 1024


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
    """

    def __init__(self, sock: socket.socket, info: EndpointInfo) -> None:
        self._socket = sock
        self.info = info
        self.closed = False

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
            f'{type(self).__name__}(uuid={self.info.uuid}, '
            f'name={self.info.name!r})'
        )

    @classmethod
    def connect(
        cls,
        host: str,
        port: int,
        token: bytes,
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
            EndpointVersionWarning: If the endpoint uses a different
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
            info = _handshake(sock, token)
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

        mismatches = Versions.current().mismatches(info.versions)
        if len(mismatches) > 0:
            warnings.warn(
                f'Endpoint {info.name} ({info.uuid}) uses different versions '
                f'than this client: {"; ".join(mismatches)}. Objects '
                'serialized in one environment may fail to deserialize in '
                f'another. See {VERSION_DOCS_URL} for details.',
                EndpointVersionWarning,
                stacklevel=2,
            )
        return cls(sock, info)

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
        if not os.path.isdir(endpoint_dir):
            raise EndpointNotFoundError(
                f'The endpoint directory {endpoint_dir} does not exist.',
            )
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
        proxystore_dir = (
            home_dir() if proxystore_dir is None else proxystore_dir
        )
        endpoint_dir = EndpointDir.from_home(proxystore_dir, name)
        if not os.path.isdir(endpoint_dir):
            raise EndpointNotFoundError(
                f'An endpoint named {name} does not exist in '
                f'{proxystore_dir}.',
            )
        return cls.from_dir(endpoint_dir, timeout=timeout)

    def close(self) -> None:
        """Close the connection."""
        if not self.closed:
            self.closed = True
            self._socket.close()

    def evict(self, key: str, endpoint: uuid.UUID | str | None = None) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
            endpoint: Optional UUID of remote endpoint to forward operation to.

        Raises:
            ValueError: If `endpoint` is not a valid UUID.
            EndpointError: If the request fails.
        """
        self._request(Op.EVICT, key, endpoint)

    def exists(
        self, key: str, endpoint: uuid.UUID | str | None = None
    ) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.
            endpoint: Optional UUID of remote endpoint to forward operation to.

        Returns:
            If an object associated with the key exists.

        Raises:
            ValueError: If `endpoint` is not a valid UUID.
            EndpointError: If the request fails.
        """
        _, meta, _ = self._request(Op.EXISTS, key, endpoint)
        exists = meta.get('exists')
        if not isinstance(exists, bool):
            raise EndpointProtocolError(
                'Malformed EXISTS response: missing or invalid '
                "'exists' field.",
            )
        return exists

    def get(
        self,
        key: str,
        endpoint: uuid.UUID | str | None = None,
    ) -> bytearray | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with object to retrieve.
            endpoint: Optional UUID of remote endpoint to forward operation to.

        Returns:
            Serialized object or `None` if the object does not exist.

        Raises:
            ValueError: If `endpoint` is not a valid UUID.
            EndpointError: If the request fails.
        """
        status, _, data = self._request(Op.GET, key, endpoint)
        return None if status == Status.NOT_FOUND else data

    def set(
        self,
        key: str,
        data: BytesLike,
        endpoint: uuid.UUID | str | None = None,
    ) -> None:
        """Set the serialized object associated with the key.

        Args:
            key: Key to associate with the object.
            data: Serialized object.
            endpoint: Optional UUID of remote endpoint to forward operation to.

        Raises:
            ObjectSizeExceededError: If the size of `data` exceeds the
                maximum object size of the endpoint.
            ValueError: If `endpoint` is not a valid UUID.
            EndpointError: If the request fails.
        """
        size = memoryview(data).nbytes
        max_size = self.info.max_object_size
        if max_size is not None and size > max_size:
            raise ObjectSizeExceededError(
                f'Data size ({size} bytes) exceeds the maximum object size '
                f'of the endpoint ({max_size} bytes).',
            )
        self._request(Op.SET, key, endpoint, data)

    def _request(
        self,
        op: Op,
        key: str,
        endpoint: uuid.UUID | str | None,
        data: BytesLike | None = None,
    ) -> tuple[Status, dict[str, Any], bytearray]:
        if self.closed:
            raise EndpointConnectionError(
                'Connection to the endpoint is closed.',
            )

        request = Request(key, _parse_endpoint(endpoint))
        payload = _as_bytes_view(data) if data is not None else None
        data_len = 0 if payload is None else len(payload)
        message = pack_message(op, request.to_meta(), data_len)

        try:
            if payload is None:
                self._socket.sendall(message)
            elif data_len < _COALESCE_THRESHOLD:
                self._socket.sendall(message + payload)
            else:
                self._socket.sendall(message)
                self._socket.sendall(payload)

            header, response_meta = _recv_message(self._socket)
            response_data = _recv_exactly(self._socket, header.data_len)
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
            status = Status(header.code)
        except ValueError:
            self.close()
            raise EndpointProtocolError(
                f'Endpoint returned unknown status code {header.code}.',
            ) from None
        if status in (Status.OK, Status.NOT_FOUND):
            return status, response_meta, response_data

        error = response_meta.get('error', 'no error message provided')
        description = (
            f'Endpoint returned {status.name} for {op.name} request: {error}'
        )
        if status == Status.TOO_LARGE:
            # The endpoint may close the connection because it did not read
            # the data of the request.
            self.close()
            raise ObjectSizeExceededError(description)
        raise EndpointRequestError(description)


def _missing_connection_file_message(endpoint_dir: EndpointDir) -> str:
    message = (
        'Unable to find the connection file of the endpoint in '
        f'{endpoint_dir}.'
    )
    pid = endpoint_dir.running_pid()
    if pid is None:
        return f'{message} Is the endpoint running?'
    # Endpoints started with older versions of ProxyStore (which used an
    # HTTP API) never write a connection file.
    return (
        f'{message} The endpoint process (PID {pid}) is running, so the '
        'endpoint is either still starting or was started with an older '
        'version of ProxyStore. Restart the endpoint with the same version '
        f'of ProxyStore as the client. See {VERSION_DOCS_URL} for details.'
    )


def _parse_endpoint(endpoint: uuid.UUID | str | None) -> uuid.UUID | None:
    if endpoint is None or isinstance(endpoint, uuid.UUID):
        return endpoint
    try:
        return uuid.UUID(endpoint)
    except ValueError:
        raise ValueError(f'{endpoint} is not a valid endpoint UUID.') from None


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


def _handshake(sock: socket.socket, token: bytes) -> EndpointInfo:
    hello = Hello(nonce=os.urandom(NONCE_SIZE), versions=Versions.current())
    sock.sendall(Preamble().pack() + pack_message(Op.HELLO, hello.to_meta()))

    preamble = _recv_exactly(sock, Preamble.SIZE)
    if preamble.startswith(b'HTTP/'):
        raise EndpointProtocolError(
            'The endpoint responded with HTTP, so it is likely running a '
            'version of ProxyStore older than the client that uses the HTTP '
            'API. Restart the endpoint with the same version of ProxyStore as '
            f'the client. See {VERSION_DOCS_URL} for details.',
        )
    version = Preamble.unpack(preamble).version
    if version != PROTOCOL_VERSION:
        # Only the preamble format is the same across protocol versions so
        # nothing after it can be parsed.
        raise EndpointProtocolError(
            f'Endpoint uses protocol version {version} but the client uses '
            f'protocol version {PROTOCOL_VERSION}. Use the same version of '
            f'ProxyStore for the client and endpoint. See {VERSION_DOCS_URL} '
            'for details.',
        )

    challenge = Challenge.from_meta(_recv_handshake_message(sock))
    if not verify_proof(
        token,
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

    proof = compute_proof(token, 'client', hello.nonce, challenge.nonce)
    sock.sendall(pack_message(Op.AUTH, Auth(proof).to_meta()))

    return EndpointInfo.from_meta(_recv_handshake_message(sock))


def _recv_handshake_message(sock: socket.socket) -> dict[str, Any]:
    header, meta = _recv_message(sock)
    if header.code == Status.UNAUTHORIZED:
        raise EndpointAuthError(
            'The endpoint rejected the token of the client. The endpoint may '
            'have been restarted since the connection file was read.',
        )
    elif header.code != Status.OK:
        error = meta.get('error', 'no error message provided')
        raise EndpointProtocolError(
            f'Endpoint returned status {header.code} during the handshake: '
            f'{error}',
        )
    elif header.data_len != 0:
        raise EndpointProtocolError(
            'Endpoint sent data in a handshake message.',
        )
    return meta


def _recv_message(sock: socket.socket) -> tuple[Header, dict[str, Any]]:
    header = Header.unpack(_recv_exactly(sock, Header.SIZE))
    meta = decode_meta(_recv_exactly(sock, header.meta_len))
    return header, meta


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
