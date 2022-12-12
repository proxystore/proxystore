"""UCXStore implementation."""
from __future__ import annotations

import asyncio
import logging
import signal
from multiprocessing import Process
from time import sleep
from typing import Any
from typing import NamedTuple

try:
    import ucp

    ucx_import_error = None
except ImportError as e:  # pragma: no cover
    ucx_import_error = e

import proxystore.utils as utils
from proxystore.serialize import serialize
from proxystore.serialize import deserialize
from proxystore.serialize import SerializationError
from proxystore.store.base import Store
from proxystore.store.dim.utils import get_ip_address
from proxystore.store.dim.utils import Status


ENCODING = 'UTF-8'

server_process = None
logger = logging.getLogger(__name__)


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
    _loop: asyncio.events.AbstractEventLoop

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
        global server_process

        if ucx_import_error is not None:  # pragma: no cover
            raise ucx_import_error

        logger.debug('Instantiating client and server')

        self.host = get_ip_address(interface)
        self.port = port

        self.addr = f'{self.host}:{self.port}'

        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.new_event_loop()

        if server_process is None:
            server_process = Process(target=self._start_server)
            server_process.start()
            self._loop.run_until_complete(self.server_started())

        # TODO: Verify if create_endpoint error handling will successfully
        # connect to endpoint or if error handling needs to be done here

        super().__init__(
            name,
            cache_size=cache_size,
            stats=stats,
            kwargs={'interface': interface, 'port': self.port},
        )

    def _start_server(self) -> None:
        """Launch the local UCX server (Peer) process."""
        logger.info(
            f'starting server on host {self.host} with port {self.port}',
        )

        # create server
        ps = UCXServer(self.host, self.port)
        asyncio.run(ps.launch())

        logger.info(f'Server running at address {self.addr}')

    async def server_started(self, timeout: float = 5.0) -> None:
        sleep_time = 0.01
        time_waited = 0.0

        while True:
            try:
                ep = await ucp.create_endpoint(self.host, self.port)
            except ucp._libs.exceptions.UCXNotConnected:  # pragma: no cover
                if time_waited >= timeout:
                    raise RuntimeError(
                        'Failed to connect to server within timeout '
                        f'({timeout} seconds).',
                    )
                await asyncio.sleep(sleep_time)
                time_waited += sleep_time
            else:
                break  # pragma: no cover
        await ep.send_obj(bytes(1))
        _ = await ep.recv_obj()
        await ep.close()
        assert ep.closed()

    def create_key(self, obj: Any) -> UCXStoreKey:
        return UCXStoreKey(
            ucx_key=utils.create_key(obj),
            obj_size=len(obj),
            peer=self.addr,
        )

    def evict(self, key: UCXStoreKey) -> None:
        logger.debug(f'Client issuing an evict request on key {key}.')

        event = serialize({'key': key.ucx_key, 'data': None, 'op': 'evict'})
        self._loop.run_until_complete(self.handler(event, key.peer))

        self._cache.evict(key)

    def exists(self, key: UCXStoreKey) -> bool:
        logger.debug(f'Client issuing an exists request on key {key}.')

        event = serialize(
            {'key': key.ucx_key, 'data': None, 'op': 'exists'},
        )
        return deserialize(
            self._loop.run_until_complete(self.handler(event, key.peer)),
        )

    def get_bytes(self, key: UCXStoreKey) -> bytes | None:
        res: bytes | None
        logger.debug(f'Client issuing get request on key {key}.')

        event = serialize({'key': key.ucx_key, 'data': '', 'op': 'get'})
        res = self._loop.run_until_complete(self.handler(event, key.peer))

        try:
            s = deserialize(res)

            if isinstance(s, Status) and not s.success:
                return None
            return res
        except SerializationError:
            return res

    def set_bytes(self, key: UCXStoreKey, data: bytes) -> None:
        logger.debug(
            f'Client issuing set request on key {key} with addr {self.addr}',
        )

        event = serialize({'key': key.ucx_key, 'data': data, 'op': 'set'})

        self._loop.run_until_complete(self.handler(event, self.addr))

    async def handler(self, event: bytes, addr: str) -> bytes:
        host = addr.split(':')[0]  # quick fix
        port = int(addr.split(':')[1])

        ep = await ucp.create_endpoint(host, port)

        await ep.send_obj(event)

        res = await ep.recv_obj()

        await ep.close()

        return bytes(res)  # returns bytearray by default

    def close(self) -> None:
        """Terminate Peer server process."""
        global server_process

        logger.info('Clean up requested')

        if server_process is not None:
            server_process.terminate()
            server_process.join()
            server_process = None
        ucp.reset()

        logger.debug('Clean up completed')


class UCXServer:
    """UCXServer implementation."""

    host: str
    port: int
    ucp_listener: ucp.core.Listener
    data: dict[str, bytes]

    def __init__(self, host: str, port: int) -> None:
        """Initialize the server and register all RPC calls.

        Args:
            host (str): the server host
            port (int): the server port

        """
        signal.signal(signal.SIGINT, self.close)
        signal.signal(signal.SIGTERM, self.close)

        self.host = host
        self.port = port
        self.data = {}
        self.ucp_listener = None

    def set(self, key: str, data: bytes) -> Status:
        """Obtain data from the client and store it in local dictionary.

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
            return Status(success=False, error=e)

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
        """Verifies whether key exists within local dictionary.

        Args:
            key (str): the object's key

        Returns (bytes):
            whether key exists

        """
        return key in self.data

    async def handler(self, ep: ucp.Endpoint) -> None:
        """Function handler implementation.

        Args:
            ep (ucp.Endpoint): the endpoint to communicate with.

        """
        json_kv = await ep.recv_obj()

        if json_kv == bytes(1):
            await ep.send_obj(bytes(1))
            return

        kv = deserialize(bytes(json_kv))

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

        await ep.send_obj(serialized_res)

    async def launch(self) -> None:
        """Create a listener for the handler."""
        ucp.reset()
        ucp.init()
        self.ucp_listener = ucp.create_listener(self.handler, self.port)

        while not self.ucp_listener.closed():
            await asyncio.sleep(0.1)

        self.ucp_listener = None

    def close(self, *args: Any) -> None:
        self.ucp_listener.close()

        # listener takes time to close
        # added a sleep here to try to ensure
        # that launch function terminates prior to
        # the process termination
        while not self.ucp_listener.closed():
            sleep(0.1)
