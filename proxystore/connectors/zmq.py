"""ZeroMQ-based distributed in-memory connector implementation.

Each host runs a [`ZeroMQServer`][proxystore.connectors.zmq.ZeroMQServer]
which stores objects in memory. The first
[`ZeroMQConnector`][proxystore.connectors.zmq.ZeroMQConnector] created on a
host spawns the server if one is not already running. Objects are put in
the server on the same host, and keys contain the address of that server
so connectors on other hosts can get objects directly from it.

Warning:
    The servers do not authenticate clients or encrypt data, so anyone who
    can reach a server's port can read, write, and evict objects. Only use
    this connector within trusted networks which are not publicly accessible,
    such as the interconnect of an HPC cluster.

Messages are multipart ZeroMQ messages. A request consists of a request ID
frame, a JSON header frame (e.g., `#!json {"op": "get", "obj_id": "..."}`),
and optionally a data frame. The reply echoes the request ID and contains a
JSON header frame with the status and optionally a data frame. Data frames
are sent and received without copies.
"""

from __future__ import annotations

import argparse
import atexit
import collections
import ipaddress
import json
import logging
import os
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import uuid
import warnings
import weakref
from collections.abc import Sequence
from types import FrameType
from types import TracebackType
from typing import Any
from typing import NamedTuple
from typing import Self

import zmq

from proxystore.serialize import BytesLike

logger = logging.getLogger(__name__)

_PING_INTERVAL = 0.1


class ServerTimeoutError(Exception):
    """Client timed out waiting for a response from a server."""


class ZeroMQServerError(Exception):
    """Server failed to process a request."""


class ZeroMQKey(NamedTuple):
    """Key to objects stored in a ZeroMQ server.

    Attributes:
        obj_id: Unique object ID.
        peer_host: Address of the server where the object is stored.
        peer_port: Port of the server where the object is stored.
    """

    obj_id: str
    peer_host: str
    peer_port: int


class _Reply(NamedTuple):
    header: dict[str, Any]
    data: BytesLike | None


class ZeroMQConnector:
    """ZeroMQ-based distributed in-memory connector.

    Warning:
        The servers do not authenticate clients or encrypt data, so anyone
        who can reach a server's port can read, write, and evict objects.
        Only use this connector within trusted networks which are not
        publicly accessible, such as the interconnect of an HPC cluster.

    Warning:
        Objects are stored in the memory of the server process, and there is
        no limit to the memory used. Objects are lost when the server exits.

    Note:
        The first connector created on a host spawns a
        [`ZeroMQServer`][proxystore.connectors.zmq.ZeroMQServer] in a
        new process which other connectors on the host will use. Closing the
        connector does not stop the server by default (see `clear`), but the
        server is stopped when the process which spawned it exits.

    Example:
        ```python
        from proxystore.connectors.zmq import ZeroMQConnector

        with ZeroMQConnector(port=5555) as connector:
            key = connector.put(b'value')
            assert connector.get(key) == b'value'
        ```

    Args:
        port: Port of the server on this host.
        address: Address of this host that other hosts can connect to.
            Takes precedence over `interface` if both are provided.
        interface: Network interface to get the address of this host from
            (e.g., `'eth0'`). Only supported on Linux.
        timeout: Timeout in seconds to connect to or spawn the server on this
            host.
        request_timeout: Timeout in seconds to wait for a response from a
            server before raising a
            [`ServerTimeoutError`][proxystore.connectors.zmq.ServerTimeoutError].
            When operating on multiple objects, the timeout applies to each
            response. `None` waits forever.
        clear: Stop the server on this host, if it was spawned by this
            connector, when
            [`close()`][proxystore.connectors.zmq.ZeroMQConnector.close] is
            called. This will lose all objects stored in the server.

    Raises:
        ServerTimeoutError: If a server on this host could not be connected
            to or spawned within `timeout` seconds.
    """

    def __init__(
        self,
        port: int,
        address: str | None = None,
        interface: str | None = None,
        timeout: float = 5,
        request_timeout: float | None = 60,
        clear: bool = False,
    ) -> None:
        self._address = address
        self._interface = interface
        self.port = port
        self.timeout = timeout
        self.request_timeout = request_timeout
        self.clear = clear

        if self._address is not None:
            self.address = self._address
        elif self._interface is not None:
            self.address = get_interface_address(self._interface)
        else:
            self.address = socket.gethostbyname(socket.gethostname())
            if ipaddress.ip_address(self.address).is_loopback:
                warnings.warn(
                    f'The address of this host ({self.address}) resolved '
                    'from its hostname is a loopback address so other hosts '
                    'will not be able to get objects from this host. '
                    'Specify the address or interface to use instead.',
                    stacklevel=2,
                )

        self._kill_hook: Any = None
        self.server: subprocess.Popen[bytes] | None = None
        try:
            # Check if the port is open first because ZeroMQ will silently
            # retry connecting to a port which refuses connections.
            socket.create_connection(
                (self.address, self.port),
                timeout=self.timeout,
            ).close()
            wait_for_server(self.address, self.port, timeout=self.timeout)
        except (OSError, ServerTimeoutError):
            self.server = spawn_server(
                self.address,
                self.port,
                timeout=self.timeout,
            )
        else:
            logger.info('Connected to existing server at %s', self.url)

        if self.server is not None:
            self._kill_hook = _register_kill_hook(self.server)

        self._pool = _SocketPool()

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
            f'{self.__class__.__name__}(address={self.address}, '
            f'port={self.port})'
        )

    @property
    def url(self) -> str:
        """URL of the server on this host."""
        return f'tcp://{self.address}:{self.port}'

    def _new_key(self) -> ZeroMQKey:
        return ZeroMQKey(
            obj_id=str(uuid.uuid4()),
            peer_host=self.address,
            peer_port=self.port,
        )

    def _request(
        self,
        requests: Sequence[tuple[str, ZeroMQKey, BytesLike | None]],
    ) -> list[_Reply]:
        """Send requests to servers and wait for the replies.

        All requests are sent before waiting for any replies so requests
        to the same server are pipelined and requests to different servers
        are processed concurrently.

        Args:
            requests: Sequence of the operation, key, and optional data of
                each request.

        Returns:
            Replies in the same order as `requests`.

        Raises:
            ServerTimeoutError: If a server does not reply within
                `request_timeout` seconds.
            ZeroMQServerError: If a server failed to process a request.
        """
        timeout = (
            None
            if self.request_timeout is None
            else int(self.request_timeout * 1000)
        )
        sockets: dict[str, zmq.Socket[bytes]] = {}
        pending: dict[str, dict[bytes, int]] = collections.defaultdict(dict)
        replies: list[_Reply | None] = [None] * len(requests)
        errors: list[str] = []

        try:
            for index, (op, key, data) in enumerate(requests):
                url = f'tcp://{key.peer_host}:{key.peer_port}'
                if url not in sockets:
                    sockets[url] = self._pool.acquire(url)
                request_id = uuid.uuid4().bytes
                pending[url][request_id] = index
                _send(sockets[url], request_id, _request_header(op, key), data)

            for url, request_ids in pending.items():
                sock = sockets[url]
                while request_ids:
                    if not sock.poll(timeout):
                        # Close the socket rather than returning it to the
                        # pool so later requests do not get sent to a server
                        # which is not responding.
                        sockets.pop(url).close(linger=0)
                        raise ServerTimeoutError(
                            f'Server at {url} did not respond within '
                            f'{self.request_timeout} seconds.',
                        )
                    request_id, reply = _recv(sock)
                    index_ = request_ids.pop(request_id, None)
                    if index_ is None:  # pragma: no cover
                        # Reply to a request from a previous call which
                        # raised an error before receiving all replies.
                        continue
                    replies[index_] = reply
                    if reply.header['status'] != 'ok':
                        errors.append(reply.header['error'])
        finally:
            for url, sock in sockets.items():
                self._pool.release(url, sock)

        if errors:
            raise ZeroMQServerError('; '.join(errors))

        return [reply for reply in replies if reply is not None]

    def close(self, *, clear: bool | None = None) -> None:
        """Close the connector.

        Args:
            clear: Stop the server on this host if it was spawned by
                this connector. This will lose all objects stored in the
                server. Overrides the default value of `clear` provided when
                the connector was instantiated.
        """
        self._pool.close()

        clear = self.clear if clear is None else clear
        if clear and self.server is not None:
            _kill_server(self.server)
            atexit.unregister(self._kill_hook)
            logger.info(
                'Stopped server at %s (pid=%d)',
                self.url,
                self.server.pid,
            )
            self.server = None

    def config(self) -> dict[str, Any]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        return {
            'port': self.port,
            'address': self._address,
            'interface': self._interface,
            'timeout': self.timeout,
            'request_timeout': self.request_timeout,
            'clear': self.clear,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> ZeroMQConnector:
        """Create a new connector instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.
        """
        return cls(**config)

    def evict(self, key: ZeroMQKey) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        self._request([('evict', key, None)])

    def exists(self, key: ZeroMQKey) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        (reply,) = self._request([('exists', key, None)])
        return reply.header['exists']

    def get(self, key: ZeroMQKey) -> BytesLike | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        (reply,) = self._request([('get', key, None)])
        return reply.data

    def get_batch(self, keys: Sequence[ZeroMQKey]) -> list[BytesLike | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or \
            `None` if the corresponding key does not have an associated object.
        """
        replies = self._request([('get', key, None) for key in keys])
        return [reply.data for reply in replies]

    def new_key(self, obj: BytesLike | None = None) -> ZeroMQKey:
        """Create a new key.

        Args:
            obj: Optional object which the key will be associated with.
                Ignored in this implementation.

        Returns:
            Key which can be used to retrieve an object once \
            [`set()`][proxystore.connectors.zmq.ZeroMQConnector.set] \
            has been called on the key.
        """
        return self._new_key()

    def put(self, obj: BytesLike) -> ZeroMQKey:
        """Put a serialized object in the store.

        Args:
            obj: Serialized object to put in the store.

        Returns:
            Key which can be used to retrieve the object.
        """
        key = self._new_key()
        self._request([('put', key, obj)])
        return key

    def put_batch(self, objs: Sequence[BytesLike]) -> list[ZeroMQKey]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.

        Returns:
            List of keys with the same order as `objs` which can be used to \
            retrieve the objects.
        """
        keys = [self._new_key() for _ in objs]
        self._request(
            [('put', key, obj) for key, obj in zip(keys, objs, strict=True)],
        )
        return keys

    def set(self, key: ZeroMQKey, obj: BytesLike) -> None:
        """Set the object associated with a key.

        Note:
            The [`Connector`][proxystore.connectors.protocols.Connector]
            provides write-once, read-many semantics. Thus,
            [`set()`][proxystore.connectors.zmq.ZeroMQConnector.set]
            should only be called once per key, otherwise unexpected behavior
            can occur.

        Args:
            key: Key that the object will be associated with.
            obj: Object to associate with the key.
        """
        self._request([('put', key, obj)])


class _SocketPool:
    """Pool of idle sockets connected to servers.

    ZeroMQ sockets are not thread-safe, so each request acquires a socket
    from the pool and releases it when done.
    """

    def __init__(self) -> None:
        self._context: zmq.Context[zmq.Socket[bytes]] = zmq.Context()
        self._idle: dict[str, list[zmq.Socket[bytes]]] = (
            collections.defaultdict(list)
        )
        self._lock = threading.Lock()
        self._abandoned: list[Any] = []
        _POOLS.add(self)

    def acquire(self, url: str) -> zmq.Socket[bytes]:
        with self._lock:
            if self._idle[url]:
                return self._idle[url].pop()
            sock = self._context.socket(zmq.DEALER)
        sock.setsockopt(zmq.LINGER, 0)
        sock.connect(url)
        return sock

    def release(self, url: str, sock: zmq.Socket[bytes]) -> None:
        with self._lock:
            if self._context.closed:
                sock.close()
            else:
                self._idle[url].append(sock)

    def close(self) -> None:
        with self._lock:
            for sockets in self._idle.values():
                for sock in sockets:
                    sock.close(linger=0)
            self._idle.clear()
            self._context.destroy(linger=0)

    def _reset_after_fork(self) -> None:
        # The context and sockets are shared with the parent process so they
        # cannot be used by the child, and the lock may have been held by
        # another thread of the parent when the process was forked. Closing
        # them in the child is a no-op in pyzmq, so references are kept to
        # avoid warnings about unclosed sockets when they are collected.
        self._abandoned.append((self._context, self._idle))
        self._lock = threading.Lock()
        self._idle = collections.defaultdict(list)
        self._context = zmq.Context()


_POOLS: weakref.WeakSet[_SocketPool] = weakref.WeakSet()


def _reset_pools_after_fork() -> None:
    for pool in list(_POOLS):
        pool._reset_after_fork()


if hasattr(os, 'register_at_fork'):  # pragma: no branch
    os.register_at_fork(after_in_child=_reset_pools_after_fork)


def _request_header(op: str, key: ZeroMQKey | None = None) -> bytes:
    header: dict[str, Any] = {'op': op}
    if key is not None:
        header['obj_id'] = key.obj_id
    return json.dumps(header).encode()


def _send(
    sock: zmq.Socket[bytes],
    request_id: bytes,
    header: bytes,
    data: BytesLike | None,
) -> None:
    frames: list[Any] = [request_id, header]
    if data is not None:
        frames.append(data)
    sock.send_multipart(frames, copy=False)


def _recv(sock: zmq.Socket[bytes]) -> tuple[bytes, _Reply]:
    frames = sock.recv_multipart(copy=False)
    header = json.loads(frames[1].bytes)
    data = frames[2].buffer if len(frames) > 2 else None
    return frames[0].bytes, _Reply(header, data)


class ZeroMQServer:
    """In-memory storage and request handling for a server.

    Use [`run_server()`][proxystore.connectors.zmq.run_server] to serve
    requests from clients.
    """

    def __init__(self) -> None:
        self.data: dict[str, BytesLike] = {}

    def handle(self, frames: Sequence[zmq.Frame]) -> list[Any]:
        """Process a request.

        Args:
            frames: Frames of the request, excluding the identity frame
                added by the router socket.

        Returns:
            Frames of the reply, excluding the identity frame.
        """
        request_id = frames[0].bytes if len(frames) > 0 else b''
        try:
            header, data = self._handle(frames)
        except (TypeError, ValueError) as e:
            # JSON and Unicode decode errors are subclasses of ValueError.
            logger.debug('Failed to process request: %r', e)
            header = {'status': 'error', 'error': f'{type(e).__name__}: {e}'}
            data = None

        reply: list[Any] = [request_id, json.dumps(header).encode()]
        if data is not None:
            reply.append(data)
        return reply

    def _handle(
        self,
        frames: Sequence[zmq.Frame],
    ) -> tuple[dict[str, Any], BytesLike | None]:
        if len(frames) not in (2, 3):
            raise ValueError(
                f'Expected 2 or 3 frames in a request but got {len(frames)}.',
            )
        header = json.loads(frames[1].bytes)
        if not isinstance(header, dict):
            raise TypeError('Request header must be a JSON object.')
        op = header.get('op')

        if op == 'ping':
            return {'status': 'ok', 'pid': os.getpid()}, None

        obj_id = header.get('obj_id')
        if not isinstance(obj_id, str):
            raise TypeError('Request header must contain a string obj_id.')

        if op == 'evict':
            self.data.pop(obj_id, None)
            return {'status': 'ok'}, None
        if op == 'exists':
            return {'status': 'ok', 'exists': obj_id in self.data}, None
        if op == 'get':
            return {'status': 'ok'}, self.data.get(obj_id)
        if op == 'put':
            if len(frames) != 3:
                raise ValueError('Put request is missing the data frame.')
            self.data[obj_id] = frames[2].buffer
            return {'status': 'ok'}, None
        raise ValueError(f'Unknown operation: {op!r}.')


def run_server(
    address: str,
    port: int,
    *,
    stop: threading.Event | None = None,
    poll_interval: float = 0.1,
) -> None:
    """Serve requests from clients until stopped.

    Args:
        address: Address to bind to.
        port: Port to bind to.
        stop: Event which stops the server when set. If `None`, the server
            runs forever.
        poll_interval: Max time in seconds between checking `stop`.

    Raises:
        zmq.ZMQError: If the server fails to bind to the address and port.
    """
    stop = threading.Event() if stop is None else stop
    server = ZeroMQServer()
    context: zmq.Context[zmq.Socket[bytes]] = zmq.Context()
    sock = context.socket(zmq.ROUTER)
    sock.setsockopt(zmq.LINGER, 0)

    try:
        sock.bind(f'tcp://{address}:{port}')
        logger.info('Server listening on tcp://%s:%d', address, port)
        while not stop.is_set():
            if not sock.poll(int(poll_interval * 1000)):
                continue
            identity, *frames = sock.recv_multipart(copy=False)
            reply = server.handle(frames)
            sock.send_multipart([identity, *reply], copy=False)
    finally:
        context.destroy(linger=0)


def start_server(address: str, port: int) -> None:  # pragma: no cover
    """Run a server until SIGINT or SIGTERM is received.

    This is the entry point of the process started by
    [`spawn_server()`][proxystore.connectors.zmq.spawn_server].

    Args:
        address: Address to bind to.
        port: Port to bind to.
    """
    stop = threading.Event()

    def _handler(signum: int, frame: FrameType | None) -> None:
        stop.set()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)

    run_server(address, port, stop=stop)


def spawn_server(
    address: str,
    port: int,
    *,
    timeout: float = 5,
) -> subprocess.Popen[bytes] | None:
    """Spawn a server in a new process.

    If another process spawns a server on the same address and port at the
    same time, the server spawned by this call will fail to bind and exit,
    and `None` is returned because the other server can be used instead.

    Args:
        address: Address the server will bind to.
        port: Port the server will bind to.
        timeout: Max time in seconds to wait for the server to start.

    Returns:
        The process running the server or `None` if the server was spawned \
        by a different process.

    Raises:
        ServerTimeoutError: If the server does not start within `timeout`
            seconds.
    """
    process = subprocess.Popen(
        [
            sys.executable,
            '-m',
            'proxystore.connectors.zmq',
            '--address',
            address,
            '--port',
            str(port),
        ],
    )

    try:
        pid = wait_for_server(address, port, timeout=timeout)
    except ServerTimeoutError:
        _kill_server(process)
        raise

    if pid != process.pid:
        # The server spawned by this call will fail to bind because another
        # server is already running. Wait for it to exit to avoid leaving a
        # zombie process.
        _kill_server(process)
        logger.info(
            'Using server at tcp://%s:%d spawned by another process (pid=%d)',
            address,
            port,
            pid,
        )
        return None

    logger.info(
        'Spawned server at tcp://%s:%d (pid=%d)',
        address,
        port,
        process.pid,
    )
    return process


def wait_for_server(address: str, port: int, timeout: float = 5) -> int:
    """Wait until a server responds.

    Args:
        address: Address of the server.
        port: Port of the server.
        timeout: Max time in seconds to wait for the server to respond.

    Returns:
        The process ID of the server.

    Raises:
        ServerTimeoutError: If the server does not respond within `timeout`
            seconds.
    """
    deadline = time.monotonic() + timeout
    with zmq.Context() as context, context.socket(zmq.DEALER) as sock:
        sock.setsockopt(zmq.LINGER, 0)
        sock.connect(f'tcp://{address}:{port}')
        request_id = uuid.uuid4().bytes
        _send(sock, request_id, _request_header('ping'), None)

        while (remaining := deadline - time.monotonic()) > 0:
            if sock.poll(int(min(remaining, _PING_INTERVAL) * 1000)):
                reply_id, reply = _recv(sock)
                if reply_id == request_id:  # pragma: no branch
                    return reply.header['pid']

    raise ServerTimeoutError(
        f'Server at tcp://{address}:{port} did not respond within '
        f'{timeout} seconds.',
    )


def _kill_server(
    process: subprocess.Popen[bytes],
    timeout: float = 5,
) -> None:
    process.terminate()
    try:
        process.wait(timeout)
    except subprocess.TimeoutExpired:  # pragma: no cover
        process.kill()
        process.wait()


def _register_kill_hook(process: subprocess.Popen[bytes]) -> Any:
    def _kill_on_exit() -> None:  # pragma: no cover
        _kill_server(process)

    atexit.register(_kill_on_exit)
    return _kill_on_exit


def get_interface_address(interface: str) -> str:
    """Get the IPv4 address of a network interface.

    Warning:
        This function is only supported on Linux.

    Args:
        interface: Name of the network interface (e.g., `'eth0'`).

    Returns:
        The IPv4 address of the interface.

    Raises:
        NotImplementedError: If not on Linux.
        OSError: If the interface does not exist or has no IPv4 address.
    """
    if sys.platform.startswith('linux'):  # pragma: linux cover
        import fcntl

        siocgifaddr = 0x8915
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            request = struct.pack('256s', interface[:15].encode())
            result = fcntl.ioctl(s.fileno(), siocgifaddr, request)
        return socket.inet_ntoa(result[20:24])

    raise NotImplementedError(
        'Getting the address of an interface is only supported on Linux. '
        'Specify the address instead.',
    )


def _main(argv: Sequence[str] | None = None) -> int:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description='Run a ZeroMQConnector server.',
    )
    parser.add_argument('--address', required=True)
    parser.add_argument('--port', required=True, type=int)
    args = parser.parse_args(argv)

    try:
        start_server(args.address, args.port)
    except zmq.ZMQError as e:
        if e.errno == zmq.EADDRINUSE:
            # Another process spawned a server at the same time.
            return 1
        raise
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(_main())
