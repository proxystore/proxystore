"""ZeroMQ implementation."""
from __future__ import annotations

import asyncio
import logging
import signal
import time
from multiprocessing import Process
from typing import Any
from typing import NamedTuple

try:
    import zmq
    import zmq.asyncio

    zmq_import_error = None
except ImportError as e:  # pragma: no cover
    zmq_import_error = e

import proxystore.utils as utils
from proxystore.serialize import deserialize
from proxystore.serialize import SerializationError
from proxystore.serialize import serialize
from proxystore.store.base import Store
from proxystore.store.dim.utils import get_ip_address
from proxystore.store.dim.utils import Status

MAX_CHUNK_LENGTH = 64 * 1024
MAX_SIZE_DEFAULT = 1024**3

logger = logging.getLogger(__name__)
server_process = None


class ZeroMQStoreKey(NamedTuple):
    """Key to objects in a ZeroMQStore."""

    zmq_key: str
    """Unique object key."""
    obj_size: int
    """Object size in bytes."""
    peer: str
    """Peer where object is located."""


class ZeroMQStore(Store[ZeroMQStoreKey]):
    """Distributed in-memory store using Zero MQ.

    This client will initialize a local ZeroMQ server (Peer service) that it
    will store data to.

    Args:
        name: Name of the store instance.
        interface: The network interface to use.
        port: The desired port for communication.
        max_size: The maximum size to be communicated via zmq.
        cache_size: Size of LRU cache (in # of objects). If 0, the cache is
            disabled. The cache is local to the Python process.
        stats: Collect stats on store operations.
    """

    addr: str
    provider_id: int
    context: zmq.asyncio.Context
    socket: zmq.asyncio.Socket
    max_size: int
    chunk_size: int
    _loop: asyncio.events.AbstractEventLoop

    def __init__(
        self,
        name: str,
        *,
        interface: str,
        port: int,
        max_size: int = MAX_SIZE_DEFAULT,
        cache_size: int = 16,
        stats: bool = False,
    ) -> None:
        global server_process

        # ZMQ is not a default dependency so we don't want to raise
        # an error unless the user actually tries to use this code
        if zmq_import_error is not None:  # pragma: no cover
            raise zmq_import_error

        logger.debug('Instantiating client and server')

        self.max_size = max_size
        self.chunk_size = MAX_CHUNK_LENGTH

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

        super().__init__(
            name,
            cache_size=cache_size,
            stats=stats,
            kwargs={
                'interface': interface,
                'port': self.port,
                'max_size': self.max_size,
            },
        )

    def __del__(self) -> None:
        # https://github.com/zeromq/pyzmq/issues/1512
        self.socket.close()
        self.context.term()

    def _start_server(self) -> None:
        """Launch the local ZeroMQ server process."""
        logger.info(
            f'starting server on host {self.host} with port {self.port}',
        )

        ps = ZeroMQServer(self.host, self.port, self.max_size)
        asyncio.run(ps.launch())

    def create_key(self, obj: Any) -> ZeroMQStoreKey:
        return ZeroMQStoreKey(
            zmq_key=utils.create_key(obj),
            obj_size=len(obj),
            peer=self.addr,
        )

    def evict(self, key: ZeroMQStoreKey) -> None:
        logger.debug(f'Client issuing an evict request on key {key}.')

        event = serialize(
            {'key': key.zmq_key, 'data': None, 'op': 'evict'},
        )
        self._loop.run_until_complete(self.handler(event, key.peer))
        self._cache.evict(key)

    def exists(self, key: ZeroMQStoreKey) -> bool:
        logger.debug(f'Client issuing an exists request on key {key}.')

        event = serialize(
            {'key': key.zmq_key, 'data': None, 'op': 'exists'},
        )
        return deserialize(
            self._loop.run_until_complete(self.handler(event, key.peer)),
        )

    def get_bytes(self, key: ZeroMQStoreKey) -> bytes | None:
        logger.debug(f'Client issuing get request on key {key}')

        event = serialize(
            {'key': key.zmq_key, 'data': None, 'op': 'get'},
        )
        res = self._loop.run_until_complete(self.handler(event, key.peer))

        try:
            s = deserialize(res)

            if isinstance(s, Status) and not s.success:
                return None
            return res
        except SerializationError:
            return res

    def set_bytes(self, key: ZeroMQStoreKey, data: bytes) -> None:
        logger.debug(
            f'Client issuing set request on key {key} with addr {self.addr}',
        )

        event = serialize(
            {'key': key.zmq_key, 'data': data, 'op': 'set'},
        )
        self._loop.run_until_complete(self.handler(event, self.addr))

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
        """Terminate Peer server process."""
        global server_process

        logger.info('Clean up requested')

        if server_process is not None:  # pragma: no cover
            server_process.terminate()
            server_process.join()
            server_process = None

        logger.debug('Clean up completed')


class ZeroMQServer:
    """ZeroMQServer implementation.

    Args:
        host: IP address of the location to start the server.
        port: The port to initiate communication on.
        max_size: The maximum size allowed for zmq communication.
    """

    host: str
    port: int
    max_size: int
    chunk_size: int
    data: dict[str, bytes]

    def __init__(self, host: str, port: int, max_size: int) -> None:
        self.host = host
        self.port = port
        self.max_size = max_size
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
                for pkv in await self.socket.recv_multipart():
                    assert isinstance(pkv, bytes)

                    if pkv == b'ping':
                        self.socket.send(b'pong')
                        continue

                    kv = deserialize(pkv)

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
            except asyncio.exceptions.CancelledError:  # pragma: no cover
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
