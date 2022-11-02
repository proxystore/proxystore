"""Websockets implementation."""
from __future__ import annotations

import asyncio
import logging
import pickle
import signal
from multiprocessing import Process
from time import sleep
from typing import Any
from typing import NamedTuple

from websockets.client import connect
from websockets.server import serve

import proxystore.utils as utils
from proxystore.store.base import Store
from proxystore.store.dim.utils import get_ip_address

ENCODING = 'UTF-8'

logger = logging.getLogger(__name__)
server_process = None


class WebsocketStoreKey(NamedTuple):
    """Key to objects in a WebsocketStore."""

    websocket_key: str
    obj_size: int
    peer: str


class WebsocketStore(Store[WebsocketStoreKey]):
    """Distributed in-memory store using websockets."""

    addr: str
    provider_id: int
    max_size: int
    chunk_size: int
    _logger: logging.Logger

    def __init__(
        self,
        name: str,
        *,
        interface: str,
        port: int,
        max_size: int = 1024**3,
        cache_size: int = 16,
        stats: bool = False,
    ) -> None:
        """Initialization of a Websocket client.

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

        logger.debug('Instantiating client and server')

        self.max_size = max_size
        self.chunk_size = 16 * 1024

        self.host = get_ip_address(interface)
        self.port = port

        self.addr = f'ws://{self.host}:{self.port}'

        if server_process is None:
            server_process = Process(target=self._start_server)
            server_process.start()

        # allocate some time to start the server process
        sleep(0.2)

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
        """Launch the local Margo server (Peer) process."""
        logger.info(
            f'starting server on host {self.host} with port {self.port}',
        )

        # create server
        ps = WebsocketServer(
            self.host,
            self.port,
            self.max_size,
            chunk_size=self.chunk_size,
        )
        asyncio.run(ps.launch())

        logger.info('Server running at address %s', self.addr)

    def create_key(self, obj: Any) -> WebsocketStoreKey:
        return WebsocketStoreKey(
            websocket_key=utils.create_key(obj),
            obj_size=len(obj),
            peer=self.addr,
        )

    def evict(self, key: WebsocketStoreKey) -> None:
        logger.debug('Client issuing an evict request on key %s', key)

        event = pickle.dumps(
            {'key': key.websocket_key, 'data': None, 'op': 'evict'},
        )
        asyncio.run(self.handler(event, key.peer))
        self._cache.evict(key)

    def exists(self, key: WebsocketStoreKey) -> bool:
        logger.debug('Client issuing an exists request on key %s', key)

        event = pickle.dumps(
            {'key': key.websocket_key, 'data': None, 'op': 'exists'},
        )
        return bool(
            int(asyncio.run(self.handler(event, key.peer))),
        )

    def get_bytes(self, key: WebsocketStoreKey) -> bytes | None:
        logger.debug('Client issuing get request on key %s', key)

        event = pickle.dumps(
            {'key': key.websocket_key, 'data': None, 'op': 'get'},
        )
        res = asyncio.run(self.handler(event, key.peer))

        if res == b'ERROR':
            return None
        return res

    def set_bytes(self, key: WebsocketStoreKey, data: bytes) -> None:
        logger.debug(
            'Client issuing set request on key %s with addr %s',
            key,
            self.addr,
        )

        event = pickle.dumps(
            {'key': key.websocket_key, 'data': data, 'op': 'set'},
        )
        asyncio.run(self.handler(event, self.addr))

    async def handler(self, event: bytes, addr: str) -> Any:
        """Websocket handler function implementation.

        Args:
            event (bytes): a pickled dictionary consisting of the data,
                its key and the operation to perform on the data
            addr (str): the address of the server to connect to

        Returns (Any):
            the result of the operation on the data

        """
        async with connect(
            addr,
            max_size=self.max_size,
        ) as websocket:
            await websocket.send(utils.chunk_bytes(event, self.chunk_size))
            res = await websocket.recv()

        return res

    def close(self) -> None:
        """Terminate Peer server process."""
        global server_process

        logger.info('Clean up requested')

        if server_process is not None:
            server_process.terminate()
            server_process = None

        logger.debug('Clean up completed')


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
        chunk_size: int = 64 * 1024,
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
        self.chunk_size = chunk_size
        self.data = {}
        super().__init__()

    def set(self, key: str, data: bytes) -> bytes:
        """Obtain and store locally data from client.

        Args:
            key (str): object key to use
            data (bytes): data to store

        Returns (bytes):
            That the operation has successfully completed
        """
        self.data[key] = data
        return bytes(str(1), encoding=ENCODING)

    def get(self, key: str) -> bytes:
        """Return data at a given key back to the client.

        Args:
            key (str): the object key

        Returns (bytes):
            The data associated with provided key
        """
        try:
            return self.data[key]
        except KeyError:
            return b'ERROR'

    def evict(self, key: str) -> bytes:
        """Remove key from local dictionary.

        Args:
            key (str): the object to evict's key

        Returns (bytes):
            That the evict operation has been successful
        """
        try:
            del self.data[key]
            return bytes(str(1), encoding=ENCODING)
        except KeyError:
            return b'ERROR'

    def exists(self, key: str) -> bytes:
        """Verifies whether key exists within local dictionary.

        Args:
            key (str): the object's key

        Returns (bytes):
            whether key exists
        """
        return bytes(str(int(key in self.data)), encoding=ENCODING)

    async def handler(self, websocket: Any) -> None:
        """The handler implementation for the websocket server.

        Args:
            websocket (Any): the websocket to connect to

        """
        pkv = await websocket.recv()
        kv = pickle.loads(pkv)

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
            else:
                func = self.evict
            res = func(key)

        await websocket.send(utils.chunk_bytes(res, self.chunk_size))

    async def launch(self) -> None:
        """Launch the server."""
        loop = asyncio.get_running_loop()
        stop = loop.create_future()

        loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
        loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)

        try:
            async with serve(
                self.handler,
                self.host,
                self.port,
                max_size=self.max_size,
            ):
                await stop  # run forever

        except OSError:
            logger.warning('Server already exists')
