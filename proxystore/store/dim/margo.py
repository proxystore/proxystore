"""MargoStore implementation."""
from __future__ import annotations

import logging
from enum import Enum
from multiprocessing import Process
from os import getpid
from typing import Any
from typing import NamedTuple

try:
    import pymargo
    import pymargo.bulk as bulk
    from pymargo.bulk import Bulk
    from pymargo.core import Address
    from pymargo.core import Engine
    from pymargo.core import Handle
    from pymargo.core import MargoException
    from pymargo.core import RemoteFunction

    pymargo_import_error = None
except ImportError as e:  # pragma: no cover
    pymargo_import_error = e


import proxystore.utils as utils
from proxystore.serialize import deserialize
from proxystore.serialize import serialize
from proxystore.store.base import Store
from proxystore.store.dim.utils import get_ip_address
from proxystore.store.dim.utils import Status

server_process: Process | None = None
client_pids: set[int] = set()
logger = logging.getLogger(__name__)
engine: Engine | None = None
_rpcs: dict[str, RemoteFunction]


class Protocol(Enum):
    """Available Mercury plugins and transports."""

    OFI_TCP = 'ofi+tcp'
    """libfabric tcp provider (TCP/IP)"""
    OFI_VERBS = 'ofi+verbs'
    """libfabric Verbs provider (InfiniBand or RoCE)"""
    OFI_GNI = 'ofi+gni'
    """libfabric GNI provider (Cray Aries)"""
    UCX_TCP = 'ucx+tcp'
    """UCX TCP/IP"""
    UCX_VERBS = 'ucx+verbs'
    """UCX Verbs"""
    SM_SHM = 'sm+shm'
    """Shared memory shm"""
    BMI_TCP = 'bmi+tcp'
    """BMI tcp module (TCP/IP)"""


class MargoStoreKey(NamedTuple):
    """Key to objects in a MargoStore."""

    margo_key: str
    """Unique object key."""
    obj_size: int
    """Object size in bytes."""
    peer: str
    """Peer where object is located."""


class MargoStore(Store[MargoStoreKey]):
    """MargoStore implementation for intrasite communication."""

    host: str
    addr: str
    protocol: Protocol
    engine: Engine
    _mochi_addr: Address
    _rpcs: dict[str, RemoteFunction]
    _pid: int

    # TODO : make host optional and try to get infiniband path automatically
    def __init__(
        self,
        name: str,
        *,
        interface: str,
        port: int,
        protocol: Protocol = Protocol.OFI_VERBS,
        cache_size: int = 16,
        stats: bool = False,
    ) -> None:
        """Initialize a Margo client to issue RPCs to the Margo server.

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
        global client_pids
        global engine
        global _rpcs

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

        if engine is None:
            # start client
            engine = Engine(
                self.protocol,
                mode=pymargo.client,
                use_progress_thread=True,
            )

            _rpcs = {
                'set': engine.register('set'),
                'get': engine.register('get'),
                'exists': engine.register('exists'),
                'evict': engine.register('evict'),
            }

        self.engine = engine
        self._rpcs = _rpcs

        self.server_started()

        self._pid = getpid()
        client_pids.add(self._pid)

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
        receiver = MargoServer(server_engine)
        server_engine.register('get', receiver.get)
        server_engine.register('set', receiver.set)
        server_engine.register('exists', receiver.exists)
        server_engine.register('evict', receiver.evict)
        server_engine.wait_for_finalize()

    def server_started(self) -> None:  # pragma: no cover
        """Loop until server has started."""
        logger.debug('Checking if server has started')
        while True:
            assert engine is not None
            try:
                self._mochi_addr = engine.lookup(self.addr)
                break
            except MargoException:
                pass

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
        buff = bytearray(4)  # equivalent to malloc

        blk = self.engine.create_bulk(buff, bulk.write_only)

        self.call_rpc_on(
            self.engine,
            key.peer,
            self._rpcs['exists'],
            blk,
            key.margo_key,
            len(buff),
        )

        return bool(int(deserialize(bytes(buff))))

    def get_bytes(self, key: MargoStoreKey) -> bytes | None:
        logger.debug(f'Client issuing get request on key {key}')

        buff = bytearray(key.obj_size)
        blk = self.engine.create_bulk(buff, bulk.read_write)
        s = self.call_rpc_on(
            self.engine,
            key.peer,
            self._rpcs['get'],
            blk,
            key.margo_key,
            key.obj_size,
        )

        if not s.success:
            logger.error(f'{s.error}')
            return None
        return bytes(buff)

    def set_bytes(self, key: MargoStoreKey, data: bytes) -> None:
        logger.debug(f'Client {self.addr} issuing set request on key {key}')
        blk = self.engine.create_bulk(data, bulk.read_only)
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
        global client_pids
        global engine

        client_pids.discard(self._pid)

        logger.info('Clean up requested')

        if len(client_pids) == 0 and server_process is not None:
            engine = None
            self._mochi_addr.shutdown()
            self.engine.finalize()
            server_process.join()
            server_process = None

    @staticmethod
    def call_rpc_on(
        engine: Engine,
        addr: str,
        rpc: RemoteFunction,
        array_str: Bulk,
        key: str,
        size: int,
    ) -> Status:
        """Initiate the desired RPC call on the specified provider.

        Arguments:
            engine (Engine): The client-side engine
            addr (str): The address of Margo provider to access
                        (e.g. tcp://172.21.2.203:6367)
            rpc (RemoteFunction): the rpc to issue to the server
            array_str (Bulk): the serialized data/buffer to send
                             to the server.
            key (str): the identifier of the data stored on the server
            size (int): the size of the the data

        Returns:
            A string denoting whether the communication was successful

        """
        server_addr = engine.lookup(addr)
        return deserialize(rpc.on(server_addr)(array_str, size, key))


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

        logger.debug('Server initialized')

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

        handle.respond(serialize(s))

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
        except KeyError as error:
            logger.error(f'key {error} not found.')
            s = Status(False, error)

        handle.respond(serialize(s))

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

        handle.respond(serialize(s))

    def exists(
        self,
        handle: Handle,
        bulk_str: str,
        bulk_size: int,
        key: str,
    ) -> None:
        """Check if key exists within local dictionary.

        Args:
            handle (Handle): the client issuing the requests' handle
            bulk_str (str): the shared buffer
            bulk_size (int): the size of the shared buffer
            key (str): the identifier of the data

        """
        logger.debug(f'Received exists RPC for key {key}')

        s = Status(True, None)

        # converting to int then string because length appears to be 7 for
        # True with pickle protocol 4 and cannot always guarantee that that
        # protocol will be selected
        local_array = serialize(str(int(key in self.data)))
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

        handle.respond(serialize(s))


def when_finalize() -> None:
    """Print a statement advising that engine finalization was triggered."""
    logger.info('Finalize was called. Cleaning up.')
