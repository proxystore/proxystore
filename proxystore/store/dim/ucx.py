"""UCXStore implementation."""
from __future__ import annotations

import asyncio
import logging
import pickle

try:
    import ucp

    ucx_import_error = None
except ImportError as e:
    ucx_import_error = e

from multiprocessing import Process
from time import sleep
from typing import Any, NamedTuple

import proxystore.utils as utils
from proxystore.store.base import Store
from proxystore.store.dim.utils import get_ip_address


class UCXStoreKey(NamedTuple):
    """Key to objects in a MargoStore."""

    ucx_key: str
    obj_size: int
    peer: str


class UCXStore(Store[UCXStoreKey]):
    """Implementation for the client-facing component of UCXStore."""

    addr: str
    host: str
    port: int
    server: Process
    _loop: Any
    _logger: logging.Logger

    # TODO : make host optional and try to get infiniband path automatically
    def __init__(
        self,
        name: str,
        *,
        interface: str,
        port: int,
        cache_size: int = 16,
        stats: bool = False,
    ) -> None:
        """Initialization of a UCX client to issue RPCs to the UCX server.

        This client will initialize a local UCX server (Peer service) to
        store data to.

        Args:
            name (str): name of the store instance.
            interface (str): The network interface to use
            port (int): the desired port for the UCX server
            cache_size (int): size of LRU cache (in # of objects). If 0,
                the cache is disabled. The cache is local to the Python
                process (default: 16).
            stats (bool): collect stats on store operations (default: False).
        """
        if ucx_import_error is not None:
            print('UCX import error is not None')
            raise ucx_import_error

        self._logger = logging.getLogger(type(self).__name__)
        self._logger.debug('Instantiating client and server')

        self.host = get_ip_address(interface)
        self.port = port

        self.addr = f'{ucp.get_address(ifname=interface)}:{self.port}'

        # self._loop = asyncio.get_event_loop()
        self._server = Process(target=self._start_server)
        self._server.start()

        # allocate some time to start the server process
        sleep(2)

        super().__init__(
            name,
            cache_size=cache_size,
            stats=stats,
            kwargs={'interface': interface, 'port': self.port},
        )

    def _start_server(self) -> None:
        """Launch the local UCX server (Peer) process."""
        print(f'starting server on host {self.host} with port {self.port}')

        # create server
        ps = UCXServer(self.host, self.port)
        asyncio.run(ps.launch())

        self._logger.info('Server running at address %s', self.addr)

    def create_key(self, obj: Any) -> UCXStoreKey:
        return UCXStoreKey(
            ucx_key=utils.create_key(obj),
            obj_size=len(obj),
            peer=self.addr,
        )

    def evict(self, key: UCXStoreKey) -> None:
        self._logger.debug('Client issuing an evict request on key %s', key)

        event = pickle.dumps({'key': key.ucx_key, 'data': None, 'op': 'evict'})
        res = asyncio.run(self.handler(event, key.peer))

        if res == bytes('ERROR', 'utf-8'):
            res = None

        self._cache.evict(key)

    def exists(self, key: UCXStoreKey) -> bool:
        self._logger.debug('Client issuing an exists request on key %s', key)

        event = pickle.dumps(
            {'key': key.ucx_key, 'data': None, 'op': 'exists'},
        )
        return bool(int(asyncio.run(self.handler(event, key.peer))))

    def get_bytes(self, key: UCXStoreKey) -> bytes | None:
        self._logger.debug('Client issuing get request on key %s', key)

        event = pickle.dumps({'key': key.ucx_key, 'data': '', 'op': 'get'})
        res = asyncio.run(self.handler(event, key.peer))

        if res == bytes('ERROR', 'utf-8'):
            res = None
        return res

    def set_bytes(self, key: UCXStoreKey, data: bytes) -> None:
        self._logger.debug(
            'Client issuing set request on key %s with addr %s',
            key,
            self.addr,
        )

        event = pickle.dumps({'key': key.ucx_key, 'data': data, 'op': 'set'})

        asyncio.run(self.handler(event, self.addr))

    async def handler(self, event: bytes, addr: str) -> Any:
        host = addr.split(':')[0]  # quick fix
        port = int(addr.split(':')[1])

        ep = await ucp.create_endpoint(host, port)

        await ep.send_obj(event)

        if 'close' == pickle.loads(event)['op']:
            return

        res = await ep.recv_obj()

        if isinstance(res, bytearray):
            res = bytes(res)

        await ep.close()
        assert ep.closed()

        return res

    def close(self) -> None:
        """Terminate Peer server process."""
        self._logger.info('Clean up requested')
        self._server.terminate()
        self._logger.debug('Clean up completed')


class UCXServer:
    """UCXServer implementation."""

    host: str
    port: int
    lf: Any
    data: dict[str, bytes]

    def __init__(self, host: str, port: int) -> None:
        """Initialize the server and register all RPC calls.

        Args:
            host (str): the server host
            port (int): the server port

        """
        self._logger = logging.getLogger(type(self).__name__)
        self.host = host
        self.port = port
        self.lf = None

    def set(self, key: str, data: bytes) -> bytes:
        """Obtain data from the client and store it in local dictionary.

        Args:
            key (str): object key to use
            data (bytes): data to store
        Returns (bytes):
            That the operation has successfully completed
        """
        self.data[key] = data
        return bytes('1', 'utf-8')

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
            return bytes('ERROR', 'utf-8')

    def evict(self, key: str) -> bytes:
        """Remove key from local dictionary.

        Args:
            key (str): the object to evict's key

        Returns (bytes):
            That the evict operation has been successful

        """
        try:
            del self.data[key]
            return bytes('1', 'utf-8')
        except KeyError:
            return bytes('ERROR', 'utf-8')

    def exists(self, key: str) -> bytes:
        """Verifies whether key exists within local dictionary.

        Args:
            key (str): the object's key

        Returns (bytes):
            whether key exists

        """
        return bytes(str(int(key in self.data)), 'utf-8')

    async def handler(self, ep: Any) -> None:
        """Function handler implementation.

        Args:
            ep (Any): the endpoint to communicate with.

        """
        json_kv = await ep.recv_obj()
        kv = pickle.loads(json_kv)

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

        await ep.send_obj(res)

    async def launch(self) -> None:
        """Create a listener for the handler."""
        self.lf = ucp.create_listener(self.handler, self.port)

        while not self.lf.closed():
            await asyncio.sleep(0.1)
