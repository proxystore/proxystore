"""Websockets implementation."""
from __future__ import annotations

import asyncio
import logging
import signal
from multiprocessing import Process
from typing import Any
from typing import NamedTuple

try:
    from websockets.client import connect
    from websockets.server import serve
    from websockets.server import WebSocketServerProtocol

    websockets_import_error = None
except ImportError as e:  # pragma: no cover
    websockets_import_error = e

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


class WebsocketStoreKey(NamedTuple):
    """Key to objects in a WebsocketStore."""

    websocket_key: str
    """Unique object key."""
    obj_size: int
    """Object size in bytes."""
    peer: str
    """Peer where object is located."""


class WebsocketStore(Store[WebsocketStoreKey]):
    """Distributed in-memory store using websockets."""

    addr: str
    provider_id: int
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
        """Initialize a Websocket client.

        This client will initialize a local Websocket
        server (Peer service) that it will store data to.

        Args:
            name (str): name of the store instance.
            interface (str): the network interface to use
            port (int): the desired port for communication
            max_size (int): the maximum size to be
                communicated via websockets (default: 1G)
            cache_size (int): size of LRU cache (in # of objects). If 0,
                the cache is disabled. The cache is local to the Python
                process (default: 16).
            stats (bool): collect stats on store operations (default: False).

        """
        global server_process

        # Websockets is not a default dependency so we don't want to raise
        # an error unless the user actually tries to use this code
        if websockets_import_error is not None:  # pragma: no cover
            raise websockets_import_error

        logger.debug('Instantiating client and server')

        self.max_size = max_size
        self.chunk_size = MAX_CHUNK_LENGTH

        self.host = get_ip_address(interface)
        self.port = port

        self.addr = f'ws://{self.host}:{self.port}'

        if server_process is None:
            server_process = Process(target=self._start_server)
            server_process.start()

        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.new_event_loop()
        self._loop.run_until_complete(self.server_started())

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

    def _start_server(self) -> None:
        """Launch the local Websocket server process."""
        logger.info(
            f'starting server on host {self.host} with port {self.port}',
        )

        # create server
        ps = WebsocketServer(
            self.host,
            self.port,
            self.max_size,
        )

        asyncio.run(ps.launch())

    def create_key(self, obj: Any) -> WebsocketStoreKey:
        return WebsocketStoreKey(
            websocket_key=utils.create_key(obj),
            obj_size=len(obj),
            peer=self.addr,
        )

    def evict(self, key: WebsocketStoreKey) -> None:
        logger.debug(f'Client issuing an evict request on key {key}.')

        event = serialize(
            {'key': key.websocket_key, 'data': None, 'op': 'evict'},
        )
        self._loop.run_until_complete(self.handler(event, key.peer))
        self._cache.evict(key)

    def exists(self, key: WebsocketStoreKey) -> bool:
        logger.debug(f'Client issuing an exists request on key {key}.')

        event = serialize(
            {'key': key.websocket_key, 'data': None, 'op': 'exists'},
        )
        return deserialize(
            self._loop.run_until_complete(self.handler(event, key.peer)),
        )

    def get_bytes(self, key: WebsocketStoreKey) -> bytes | None:
        logger.debug('Client issuing get request on key {key}')

        event = serialize(
            {'key': key.websocket_key, 'data': None, 'op': 'get'},
        )
        res = self._loop.run_until_complete(self.handler(event, key.peer))

        try:
            s = deserialize(res)

            if isinstance(s, Status) and not s.success:
                return None
            return res
        except SerializationError:
            return res

    def set_bytes(self, key: WebsocketStoreKey, data: bytes) -> None:
        logger.debug(
            f'Client issuing set request on key {key} with addr {self.addr}',
        )

        event = serialize(
            {'key': key.websocket_key, 'data': data, 'op': 'set'},
        )
        self._loop.run_until_complete(self.handler(event, self.addr))

    async def handler(self, event: bytes, addr: str) -> bytes:
        """Websocket handler function implementation.

        Args:
            event (bytes): a pickled dictionary consisting of the data,
                its key and the operation to perform on the data
            addr (str): the address of the server to connect to

        Returns (bytes):
            the result of the operation on the data

        """
        async with connect(
            addr,
            max_size=self.max_size,
        ) as websocket:
            await websocket.send(utils.chunk_bytes(event, self.chunk_size))
            res = await websocket.recv()

        assert isinstance(res, bytes)

        return res

    def close(self) -> None:
        """Terminate Peer server process."""
        global server_process

        logger.info('Clean up requested')

        if server_process is not None:
            server_process.terminate()
            server_process.join()
            server_process = None

        logger.debug('Clean up completed')

    async def server_started(self, timeout: float = 5.0) -> None:
        sleep_time = 0.01
        time_waited = 0.0

        while True:
            try:
                websocket = await connect(self.addr)
            except OSError as e:
                if time_waited >= timeout:
                    raise RuntimeError(
                        'Failed to connect to server within timeout '
                        f'({timeout} seconds).',
                    ) from e
                await asyncio.sleep(sleep_time)
                time_waited += sleep_time
            else:
                break  # pragma: no cover

        pong_waiter = await websocket.ping()
        await pong_waiter
        await websocket.close()


class WebsocketServer:
    """WebsocketServer implementation."""

    host: str
    port: int
    max_size: int
    chunk_size: int
    data: dict[str, bytes]

    def __init__(
        self,
        host: str,
        port: int,
        max_size: int,
    ) -> None:
        """Initialize the server and register all RPC calls.

        Args:
            host (str): IP address of the location to start the server.
            port (int): the port to initiate communication on.
            max_size (int): the maximum size allowed for
                websocket communication.
            chunk_size (int): the chunk size for the data (default: 64MB)

        """
        self.host = host
        self.port = port
        self.max_size = max_size
        self.chunk_size = MAX_CHUNK_LENGTH
        self.data = {}
        super().__init__()

    def set(self, key: str, data: bytes) -> Status:
        """Obtain and store locally data from client.

        Args:
            key (str): object key to use
            data (bytes): data to store

        Returns (bytes):
            That the operation has successfully completed
        """
        self.data[key] = data
        return Status(success=True, error=None)

    def get(self, key: str) -> bytes | Status:
        """Return data at a given key back to the client.

        Args:
            key (str): the object key

        Returns (bytes):
            The data associated with provided key
        """
        try:
            return self.data[key]
        except KeyError as e:
            return Status(False, e)

    def evict(self, key: str) -> Status:
        """Remove key from local dictionary.

        Args:
            key (str): the object to evict's key

        Returns (bytes):
            That the evict operation has been successful
        """
        self.data.pop(key, None)
        return Status(success=True, error=None)

    def exists(self, key: str) -> bool:
        """Check if a key exists within local dictionary.

        Args:
            key (str): the object's key

        Returns (bytes):
            whether key exists
        """
        return key in self.data

    async def handler(self, websocket: WebSocketServerProtocol) -> None:
        """Handle websocket connection requests.

        Args:
            websocket (WebSocketServerProtocol): the websocket server

        """
        async for pkv in websocket:
            assert isinstance(pkv, bytes)
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

            await websocket.send(
                utils.chunk_bytes(serialized_res, self.chunk_size),
            )

    async def launch(self) -> None:
        """Launch the server."""
        loop = asyncio.get_running_loop()
        stop = loop.create_future()

        loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
        loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)

        async with serve(
            self.handler,
            self.host,
            self.port,
            max_size=self.max_size,
        ):
            await stop  # run forever
