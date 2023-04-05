"""ZeroMQ-based distributed in-memory connector implementation."""
from __future__ import annotations

import atexit
import logging
import signal
import sys
import time
import uuid
from multiprocessing import Process
from types import TracebackType
from typing import Any
from typing import NamedTuple
from typing import Sequence

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

try:
    import zmq

    zmq_import_error = None
except ImportError as e:  # pragma: no cover
    zmq_import_error = e

import proxystore.utils as utils
from proxystore.connectors.dim.rpc import RPC
from proxystore.connectors.dim.rpc import RPCResponse
from proxystore.connectors.dim.utils import get_ip_address
from proxystore.connectors.dim.utils import Status
from proxystore.serialize import deserialize
from proxystore.serialize import SerializationError
from proxystore.serialize import serialize

MAX_CHUNK_LENGTH = 64 * 1024

logger = logging.getLogger(__name__)
server_process = None


class ZeroMQKey(NamedTuple):
    """Key to objects stored across `ZeroMQConnector`s."""

    zmq_key: str
    """Unique object key."""
    obj_size: int
    """Object size in bytes."""
    peer: str
    """Peer where object is located."""


class ZeroMQConnector:
    """ZeroMQ-based distributed in-memory connector.

    Note:
        The first instance of this connector created on a process will
        spawn a [`ZeroMQServer`][proxystore.connectors.dim.zmq.ZeroMQServer]
        that will store data. Hence, this connector just acts as an interface
        to that server.

    Args:
        interface: The network interface to use.
        port: The desired port for the spawned server.
    """

    addr: str
    provider_id: int
    context: zmq.sugar.context.Context[zmq.sugar.socket.Socket[Any]]
    socket: zmq.sugar.socket.Socket[Any]
    chunk_size: int

    def __init__(self, interface: str, port: int) -> None:
        # ZMQ is not a default dependency so we don't want to raise
        # an error unless the user actually tries to use this code
        if zmq_import_error is not None:  # pragma: no cover
            raise zmq_import_error

        logger.debug('Instantiating client and server')

        self.chunk_size = MAX_CHUNK_LENGTH

        self.interface = interface
        self.host = get_ip_address(interface)
        self.port = port

        self.addr = f'tcp://{self.host}:{self.port}'

        try:
            wait_for_server(self.host, self.port)
        except RuntimeError:
            self.server = spawn_server(self.host, self.port, timeout=0.1)

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)

    def __del__(self) -> None:
        # https://github.com/zeromq/pyzmq/issues/1512
        self.socket.close()
        self.context.term()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _send_rpc(self, rpc: Sequence[RPC]) -> list[RPCResponse]:
        """Send an RPC request to the server.

        Args:
            rpc: List of RPC requests.

        Returns:
            List of RPC responses.
        """
        responses = []

        for r in rpc:
            assert isinstance(r, RPC)
            event = serialize(rpc)
            with self.socket.connect(self.addr):
                self.socket.send_multipart(
                    list(utils.chunk_bytes(event, self.chunk_size)),
                )
                res = b''.join(self.socket.recv_multipart())

            assert isinstance(res, bytes)

            try:
                rpc_res = RPCResponse(
                    **deserialize(res),
                    operation=r.operation,
                    key=r.key,
                    exists=True,
                )
            except SerializationError:
                logger.exception('Deserialization error')
                raise
            responses.append(rpc_res)

        return responses

    def close(self, kill_server: bool = True) -> None:
        """Close the connector.

        Args:
            kill_server: Whether to kill the server process.
        """
        if kill_server and self.server is not None:
            self.server.terminate()
            self.server.join()

        self.socket.close()
        self.context.term()

    def config(self) -> dict[str, Any]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        return {'interface': self.interface, 'port': self.port}

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
        logger.debug(f'Client issuing an evict request on key {key}.')

        rpc = RPC(operation='evict', key=key.zmq_key, payload=None)

        self._send_rpc([rpc])

    def exists(self, key: ZeroMQKey) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        logger.debug(f'Client issuing an exists request on key {key}.')

        rpc = RPC(operation='exists', key=key.zmq_key, payload=None)

        response = self._send_rpc([rpc])

        if response[0].exists is not None:
            return response[0].exists
        else:
            return False

    def get(self, key: ZeroMQKey) -> bytes | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        logger.debug(f'Client issuing get request on key {key}')

        rpc = RPC(operation='get', key=key.zmq_key, payload=None)

        res = self._send_rpc([rpc])

        return res[0].result

    def get_batch(self, keys: Sequence[ZeroMQKey]) -> list[bytes | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or
            `None` if the corresponding key does not have an associated object.
        """
        return [self.get(key) for key in keys]

    def put(self, obj: bytes) -> ZeroMQKey:
        """Put a serialized object in the store.

        Args:
            obj: Serialized object to put in the store.

        Returns:
            Key which can be used to retrieve the object.
        """
        key = ZeroMQKey(
            zmq_key=str(uuid.uuid4()),
            obj_size=len(obj),
            peer=self.addr,
        )
        logger.debug(
            f'Client issuing set request on key {key} with addr {self.addr}',
        )
        rpc = RPC(operation='set', key=key.zmq_key, payload=obj)

        self._send_rpc([rpc])

        return key

    def put_batch(self, objs: Sequence[bytes]) -> list[ZeroMQKey]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.

        Returns:
            List of keys with the same order as `objs` which can be used to \
            retrieve the objects.
        """
        return [self.put(obj) for obj in objs]


class ZeroMQServer:
    """ZeroMQServer implementation.

    Args:
        host: IP address of the location to start the server.
        port: The port to initiate communication on.
    """

    host: str
    port: int
    chunk_size: int
    data: dict[str, bytes]

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.chunk_size = MAX_CHUNK_LENGTH
        self.data = {}

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(f'tcp://{self.host}:{self.port}')

    def close(self) -> None:
        """Close the server socket."""
        self.socket.close()
        self.context.term()

    def set(self, key: str, data: bytes) -> Status:
        """Obtain and store locally data from client.

        Args:
            key: Object key to use.
            data: Data to store.

        Returns:
            Operation status.
        """
        self.data[key] = data
        return Status(success=True, error=None)

    def get(self, key: str) -> bytes | Status:
        """Return data at a given key back to the client.

        Args:
            key: The object key.

        Returns:
            Operation status.
        """
        try:
            return self.data[key]
        except KeyError as e:
            return Status(False, e)

    def evict(self, key: str) -> Status:
        """Remove key from local dictionary.

        Args:
            key: The object to evict's key.

        Returns:
            Operation status.
        """
        self.data.pop(key, None)
        return Status(success=True, error=None)

    def exists(self, key: str) -> bool:
        """Check if a key exists within local dictionary.

        Args:
            key: The object's key.

        Returns:
            If the key exists.
        """
        return key in self.data

    def handler(self) -> None:
        """Handle zmq connection requests."""
        while not self.socket.closed:  # pragma: no branch
            try:
                pkv = self.socket.recv_multipart()
                kvb = b''.join(pkv)

                if kvb == b'ping':
                    self.socket.send(b'pong')
                    continue

                kv = deserialize(kvb)

                key = kv['key']
                data = kv['data']
                func = kv['op']

                if func == 'set':
                    res = self.set(key, data)
                else:
                    if func == 'get':
                        func = self.get
                    elif func == 'exists':
                        func = self.exists
                    elif func == 'evict':
                        func = self.evict
                    else:
                        raise AssertionError('Unreachable.')
                    res = func(key)

                if isinstance(res, Status) or isinstance(res, bool):
                    serialized_res = serialize(res)
                else:
                    serialized_res = res

                self.socket.send_multipart(
                    list(
                        utils.chunk_bytes(serialized_res, self.chunk_size),
                    ),
                )
            except zmq.ZMQError as e:  # pragma: no cover
                logger.exception(e)


def start_server(host: str, port: int) -> None:
    """Start a ZeroMQServer."""
    server = ZeroMQServer(host, port)

    signal.signal(signal.SIGINT, lambda *args: server.close())
    signal.signal(signal.SIGTERM, lambda *args: server.close())

    server.handler()


def wait_for_server(host: str, port: int, timeout: float = 0.1) -> None:
    """Wait until the ZeroMQServer responds.

    Args:
        host: The host of the server to ping.
        port: The port of the server to ping.
        timeout: The max time in seconds to wait for server response.

    Raises:
        RuntimeError: if the server does not respond within the timeout.
    """
    start = time.time()
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)

    socket.connect(f'tcp://{host}:{port}')
    socket.send(b'ping')

    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)

    while time.time() - start < timeout:
        event = poller.poll(int(timeout * 1000))
        if len(event) != 0:
            response = socket.recv()
            assert response == b'pong'
            socket.close()
            return

    socket.close()

    raise RuntimeError(
        'Failed to connect to server within timeout ({timeout} seconds).',
    )


def spawn_server(host: str, port: int, timeout: float) -> Process:
    """Spawn a ZeroMQServer in a separate process.

    Args:
        host: The host of the server to ping.
        port: The port of the server to ping.
        timeout: The max time in seconds to wait for server response.

    Returns:
        The process that the server is running in.
    """
    server_process = Process(target=start_server, args=(host, port))
    server_process.start()

    def _kill_on_exit() -> None:
        server_process.terminate()
        server_process.join(timeout=timeout)
        if server_process.is_alive():
            server_process.kill()
            server_process.join()

    atexit.register(_kill_on_exit)

    wait_for_server(host, port, timeout=timeout)

    return server_process
