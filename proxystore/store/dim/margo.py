"""MargoStore implementation."""
from __future__ import annotations

import logging
from multiprocessing import Process
from typing import Any
from typing import NamedTuple

try:
    import pymargo
    import pymargo.bulk as bulk
    from pymargo.core import Engine
    from pymargo.core import Handle
    from pymargo.core import RemoteFunction
    from pymargo.bulk import Bulk

    pymargo_import_error = None
except ImportError as e:  # pragma: no cover
    pymargo_import_error = e


import proxystore.utils as utils
from proxystore.store.base import Store
from proxystore.store.dim.utils import get_ip_address
from proxystore.store.dim.utils import Status

ENCODING = 'UTF-8'

server_process: Process | None = None
logger = logging.getLogger(__name__)


class MargoStoreKey(NamedTuple):
    """Key to objects in a MargoStore."""

    margo_key: str
    obj_size: int
    peer: str


class MargoStore(Store[MargoStoreKey]):
    """MargoStore implementation for intrasite communication."""

    host: str
    addr: str
    protocol: str
    engine: Engine
    _rpc: dict[str, RemoteFunction]

    # TODO : make host optional and try to get infiniband path automatically
    def __init__(
        self,
        name: str,
        *,
        interface: str,
        port: int,
        protocol: str = 'verbs',
        cache_size: int = 16,
        stats: bool = False,
    ) -> None:
        """Initialization of a Margo client to issue RPCs to the Margo server.

        This client will initialize a local Margo server (Peer service) that
        it will store data to.

        Args:
            name (str): name of the store instance.
            interface (str): The network interface to use
            port (int): the desired port for the Margo server
            protocol (str): The communication protocol to use
                            (e.g., tcp, sockets, verbs). default = "verbs"
            cache_size (int): size of LRU cache (in # of objects). If 0,
                the cache is disabled. The cache is local to the Python
                process (default: 16).
            stats (bool): collect stats on store operations (default: False).
        """
        global server_process

        # raise error if modules not properly loaded
        if pymargo_import_error is not None:  # pragma: no cover
            raise pymargo_import_error

        self.protocol = protocol

        self.host = get_ip_address(interface)
        self.port = port

        self.addr = f'{self.protocol}://{self.host}:{port}'

        if server_process is None:
            server_process = Process(target=self._start_server)
            server_process.start()

        # start client
        self.engine = Engine(self.protocol, mode=pymargo.client)

        self.server_started()

        self._rpcs = {
            'set': self.engine.register('set_bytes'),
            'get': self.engine.register('get_bytes'),
            'exists': self.engine.register('exists'),
            'evict': self.engine.register('evict'),
        }

        super().__init__(
            name,
            cache_size=cache_size,
            stats=stats,
            kwargs={
                'interface': interface,
                'port': self.port,
                'protocol': self.protocol,
            },
        )

    def _start_server(self) -> None:
        """Launch the local Margo server (Peer) process."""
        logger.info(f'starting server {self.addr}')
        server_engine = Engine(self.addr)

        logger.info(f'Server running at address {str(server_engine.addr())}')

        server_engine.on_finalize(when_finalize)
        server_engine.enable_remote_shutdown()

        # create server
        MargoServer(server_engine)

        server_engine.wait_for_finalize()

    # TODO: Verify that this actually does anything useful in integration tests
    def server_started(self) -> None:  # pragma: no cover
        """Loop until server has started."""
        while True:
            try:
                self.engine.lookup(self.addr)
            except Exception as e:
                print(e)  # don't yet know if any error will be thrown
            else:
                break

    def create_key(self, obj: Any) -> MargoStoreKey:
        return MargoStoreKey(
            margo_key=utils.create_key(obj),
            obj_size=len(obj),
            peer=self.addr,
        )

    def evict(self, key: MargoStoreKey) -> None:
        logger.debug(f'Client issuing an evict request on key {key}')
        self.call_rpc_on(
            self.engine,
            key.peer,
            self._rpcs['evict'],
            '',
            key.margo_key,
            0,
        )

        self._cache.evict(key)

    def exists(self, key: MargoStoreKey) -> bool:
        logger.debug(f'Client issuing an exists request on key {key}')
        buff = bytearray(1)  # equivalent to malloc

        blk = self.engine.create_bulk(buff, bulk.read_write)

        self.call_rpc_on(
            self.engine,
            key.peer,
            self._rpcs['exists'],
            blk,
            key.margo_key,
            len(buff),
        )
        return bool(int(bytes(buff).decode(ENCODING)))

    def get_bytes(self, key: MargoStoreKey) -> bytes | None:
        logger.debug(f'Client issuing get request on key {key}')

        buff = bytearray(key.obj_size)
        blk = self.engine.create_bulk(buff, bulk.read_write)

        success = self.call_rpc_on(
            self.engine,
            key.peer,
            self._rpcs['get'],
            blk,
            key.margo_key,
            key.obj_size,
        )

        if not success:
            return None

        return bytes(buff)

    def set_bytes(self, key: MargoStoreKey, data: bytes) -> None:
        logger.debug(f'Client {self.addr} issuing set request on key {key}')
        blk = self.engine.create_bulk(data, bulk.read_write)
        self.call_rpc_on(
            self.engine,
            self.addr,
            self._rpcs['set'],
            blk,
            key.margo_key,
            key.obj_size,
        )

    def close(self) -> None:
        """Terminate Peer server process."""
        global server_process

        logger.info('Clean up requested')
        self.engine.lookup(self.addr).shutdown()
        self.engine.finalize()

        if server_process is not None:
            server_process.terminate()
            server_process = None

    @staticmethod
    def call_rpc_on(
        engine: Engine,
        addr: str,
        rpc: RemoteFunction,
        array_str: str,
        key: str,
        size: int,
    ) -> bool:
        """Initiates the desired RPC call on the specified provider.

        Arguments:
            engine (Engine): The client-side engine
            addr (str): The address of Margo provider to access
                        (e.g. tcp://172.21.2.203:6367)
            rpc (RemoteFunction): the rpc to issue to the server
            array_str (str): the serialized data/buffer to send
                             to the server.
            key (str): the identifier of the data stored on the server
            size (int): the size of the the data

        Returns:
            A string denoting whether the communication was successful

        """
        server_addr = engine.lookup(addr)
        s = rpc.on(server_addr)(array_str, size, key)

        if not s.success:
            logger.error(f'{s.error}')

        return s.success


class MargoServer:
    """MargoServer implementation."""

    data: dict[str, bytes]
    engine: Engine

    def __init__(self, engine: Engine) -> None:
        """Initialize the server and register all RPC calls.

        Args:
            engine (Engine): the server engine created at the
                      specified network address

        """
        self.data = {}

        self.engine = engine
        self.engine.register('get', self.get)
        self.engine.register('set', self.set)
        self.engine.register('exists', self.exists)
        self.engine.register('evict', self.evict)

    def set(
        self,
        handle: Handle,
        bulk_str: Bulk,
        bulk_size: int,
        key: str,
    ) -> None:
        """Obtain data from the client and store it in local dictionary.

        Args:
            handle (Handle): the client handle
            bulk_str (Bulk): the buffer containing the data to be shared
            bulk_size (int): the size of the data being transferred
            key (str): the data key
        """
        logger.debug(f'Received set RPC for key {key}.')

        s = Status(True, None)

        try:
            local_buffer = bytearray(bulk_size)
            local_bulk = self.engine.create_bulk(local_buffer, bulk.write_only)
            self.engine.transfer(
                bulk.pull,
                handle.get_addr(),
                bulk_str,
                0,
                local_bulk,
                0,
                bulk_size,
            )
            self.data[key] = local_buffer
        except Exception as error:
            logger.error(f'An exception was caught: {error}')
            s = Status(False, error)

        handle.respond(s)

    def get(
        self,
        handle: Handle,
        bulk_str: Bulk,
        bulk_size: int,
        key: str,
    ) -> None:
        """Return data at a given key back to the client.

        Args:
            handle (Handle): The client handle
            bulk_str (Bulk): the buffer that will store shared data
            bulk_size (int): the size of the data to be received
            key (str): the data's key

        """
        logger.debug(f'Received get RPC for key {key}.')

        s = Status(True, None)

        try:
            local_array = self.data[key]
            local_bulk = self.engine.create_bulk(local_array, bulk.read_only)
            self.engine.transfer(
                bulk.push,
                handle.get_addr(),
                bulk_str,
                0,
                local_bulk,
                0,
                bulk_size,
            )
        except Exception as error:
            logger.error(f'An exception was caught: {error}')
            s = Status(False, error)

        handle.respond(s)

    def evict(
        self,
        handle: Handle,
        bulk_str: str,
        bulk_size: int,
        key: str,
    ) -> None:
        """Remove key from local dictionary.

        Args:
            handle (Handle): the client issuing the requests' handle
            bulk_str (str): the buffer containing any data to be shared
            bulk_size (int): the size of the data to share
            key (str): the identifier of the data

        """
        logger.debug(f'Received exists RPC for key {key}')

        self.data.pop(key, None)
        s = Status(True, None)

        handle.respond(s)

    def exists(
        self,
        handle: Handle,
        bulk_str: str,
        bulk_size: int,
        key: str,
    ) -> None:
        """Verifies whether key exists within local dictionary.

        Args:
            handle (Handle): the client issuing the requests' handle
            bulk_str (str): the shared buffer
            bulk_size (int): the size of the shared buffer
            key (str): the identifier of the data

        """
        logger.debug(f'Received exists RPC for key {key}')

        s = Status(True, None)

        try:
            local_array = bytes(str(int(key in self.data)), encoding=ENCODING)
            local_bulk = self.engine.create_bulk(local_array, bulk.read_only)
            size = len(local_array)
            self.engine.transfer(
                bulk.push,
                handle.get_addr(),
                bulk_str,
                0,
                local_bulk,
                0,
                size,
            )
        except Exception as error:
            logger.error(f'An exception was caught: {error}')
            s = Status(False, error)

        handle.respond(s)


def when_finalize() -> None:
    """Prints a statement advising that engine finalization was triggered."""
    logger.info('Finalize was called. Cleaning up.')
