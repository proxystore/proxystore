"""ZeroMQ-based distributed in-memory connector implementation."""
from __future__ import annotations

import asyncio
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
    import zmq.asyncio

    zmq_import_error = None
except ImportError as e:  # pragma: no cover
    zmq_import_error = e

import proxystore.utils as utils
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
    context: zmq.asyncio.Context
    socket: zmq.asyncio.Socket
    chunk_size: int
    _loop: asyncio.events.AbstractEventLoop

    def __init__(self, interface: str, port: int) -> None:
        global server_process

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

        if server_process is None:
            server_process = Process(target=self._start_server)
            server_process.start()

        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.REQ)

        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.new_event_loop()
        self._loop.run_until_complete(wait_for_server(self.host, self.port))

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

    def _start_server(self) -> None:
        """Launch the local ZeroMQ server process."""
        logger.info(
            f'starting server on host {self.host} with port {self.port}',
        )

        ps = ZeroMQServer(self.host, self.port)
        asyncio.run(ps.launch())

    async def handler(self, event: bytes, addr: str) -> bytes:
        """ZeroMQ handler function implementation.

        Args:
            event: A pickled dictionary consisting of the data,
                its key, and the operation to perform on the data.
            addr: The address of the server to connect to.

        Returns:
            The serialized result of the operation on the data.
        """
        with self.socket.connect(addr):
            await self.socket.send_multipart(
                list(utils.chunk_bytes(event, self.chunk_size)),
            )
            res = b''.join(await self.socket.recv_multipart())

        assert isinstance(res, bytes)

        return res

    def close(self) -> None:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        global server_process

        logger.info('Clean up requested')

        if server_process is not None:  # pragma: no cover
            server_process.terminate()
            server_process.join()
            server_process = None

        logger.debug('Clean up completed')

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

        event = serialize(
            {'key': key.zmq_key, 'data': None, 'op': 'evict'},
        )
        self._loop.run_until_complete(self.handler(event, key.peer))

    def exists(self, key: ZeroMQKey) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        logger.debug(f'Client issuing an exists request on key {key}.')

        event = serialize(
            {'key': key.zmq_key, 'data': None, 'op': 'exists'},
        )
        return deserialize(
            self._loop.run_until_complete(self.handler(event, key.peer)),
        )

    def get(self, key: ZeroMQKey) -> bytes | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        logger.debug(f'Client issuing get request on key {key}')

        event = serialize(
            {'key': key.zmq_key, 'data': None, 'op': 'get'},
        )
        res = self._loop.run_until_complete(self.handler(event, key.peer))

        try:
            s = deserialize(res)

            assert isinstance(s, Status)
            assert not s.success
            return None
        except SerializationError:
            return res

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

        event = serialize(
            {'key': key.zmq_key, 'data': obj, 'op': 'set'},
        )
        self._loop.run_until_complete(self.handler(event, self.addr))
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

        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(f'tcp://{self.host}:{self.port}')

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

    async def handler(self) -> None:
        """Handle zmq connection requests."""
        while not self.socket.closed:  # pragma: no branch
            try:
                pkv = await self.socket.recv_multipart()
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

                await self.socket.send_multipart(
                    list(
                        utils.chunk_bytes(serialized_res, self.chunk_size),
                    ),
                )
            except zmq.ZMQError as e:  # pragma: no cover
                logger.exception(e)
                await asyncio.sleep(0.01)
            except asyncio.CancelledError:  # pragma: no cover
                logger.debug('loop terminated')

    async def launch(self) -> None:
        """Launch the server."""
        loop = asyncio.get_running_loop()
        loop.create_future()

        loop.add_signal_handler(signal.SIGINT, self.socket.close, None)
        loop.add_signal_handler(signal.SIGTERM, self.socket.close, None)

        await self.handler()


async def wait_for_server(host: str, port: int, timeout: float = 5.0) -> None:
    """Wait until the ZeroMQServer responds.

    Args:
        host: The host of the server to ping.
        port: The port of the server to ping.
        timeout: The max time in seconds to wait for server response.

    Raises:
        RuntimeError: if the server does not respond within the timeout.
    """
    start = time.time()
    context = zmq.asyncio.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)

    with socket.connect(f'tcp://{host}:{port}'):
        await socket.send(b'ping')

        poller = zmq.asyncio.Poller()
        poller.register(socket, zmq.POLLIN)

        while time.time() - start < timeout:
            event = await poller.poll(timeout)
            if len(event) != 0:
                response = await socket.recv()
                assert response == b'pong'
                socket.close()
                return

    socket.close()

    raise RuntimeError(
        'Failed to connect to server within timeout ({timeout} seconds).',
    )
