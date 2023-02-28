"""Margo RPC-based distributed in-memory connector implementation."""
from __future__ import annotations

import logging
import uuid
from enum import Enum
from multiprocessing import Process
from os import getpid
from typing import Any
from typing import NamedTuple
from typing import Sequence

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


from proxystore.connectors.dim.utils import get_ip_address
from proxystore.connectors.dim.utils import Status
from proxystore.serialize import deserialize
from proxystore.serialize import serialize

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


class MargoKey(NamedTuple):
    """Key to objects stored across `MargoConnector`s."""

    margo_key: str
    """Unique object key."""
    obj_size: int
    """Object size in bytes."""
    peer: str
    """Peer where object is located."""


class MargoConnector:
    """Margo RPC-based distributed in-memory connector.

    Note:
        The first instance of this connector created on a process will
        spawn a [`MargoServer`][proxystore.connectors.dim.margo.MargoServer]
        that will store data. Hence, this connector just acts as an interface
        to that server.

    Args:
        interface: The network interface to use.
        port: The desired port for the spawned server.
        protocol: The communication protocol to use.
    """

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
        interface: str,
        port: int,
        protocol: Protocol = Protocol.OFI_VERBS,
    ) -> None:
        global server_process
        global client_pids
        global engine
        global _rpcs

        # raise error if modules not properly loaded
        if pymargo_import_error is not None:  # pragma: no cover
            raise pymargo_import_error

        self.protocol = protocol

        self.interface = interface
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
            engine: The client-side engine.
            addr: The address of Margo provider to access
                (e.g. tcp://172.21.2.203:6367).
            rpc: The rpc to issue to the server.
            array_str: The serialized data/buffer to send to the server.
            key: The identifier of the data stored on the server.
            size: The size of the the data.

        Returns:
            A string denoting whether the communication was successful
        """
        server_addr = engine.lookup(addr)
        return deserialize(rpc.on(server_addr)(array_str, size, key))

    def close(self) -> None:
        """Close the connector and clean up.

        Warning:
            This will terminate the server is no clients are still connected.

        Warning:
            This method should only be called at the end of the program
            when the connector will no longer be used, for example once all
            proxies have been resolved.
        """
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

    def config(self) -> dict[str, Any]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        return {
            'interface': self.interface,
            'port': self.port,
            'protocol': self.protocol,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> MargoConnector:
        """Create a new connector instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.
        """
        return cls(**config)

    def evict(self, key: MargoKey) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        logger.debug(f'Client issuing an evict request on key {key}')
        self.call_rpc_on(
            self.engine,
            key.peer,
            self._rpcs['evict'],
            '',
            key.margo_key,
            0,
        )

    def exists(self, key: MargoKey) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
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

    def get(self, key: MargoKey) -> bytes | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
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

    def get_batch(self, keys: Sequence[MargoKey]) -> list[bytes | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or
            `None` if the corresponding key does not have an associated object.
        """
        return [self.get(key) for key in keys]

    def put(self, obj: bytes) -> MargoKey:
        """Put a serialized object in the store.

        Args:
            obj: Serialized object to put in the store.

        Returns:
            Key which can be used to retrieve the object.
        """
        key = MargoKey(
            margo_key=str(uuid.uuid4()),
            obj_size=len(obj),
            peer=self.addr,
        )
        logger.debug(f'Client {self.addr} issuing set request on key {key}')
        blk = self.engine.create_bulk(obj, bulk.read_only)
        self.call_rpc_on(
            self.engine,
            self.addr,
            self._rpcs['set'],
            blk,
            key.margo_key,
            key.obj_size,
        )
        return key

    def put_batch(self, objs: Sequence[bytes]) -> list[MargoKey]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.

        Returns:
            List of keys with the same order as `objs` which can be used to
            retrieve the objects.
        """
        return [self.put(obj) for obj in objs]


class MargoServer:
    """MargoServer implementation.

    Args:
        engine: The server engine created at the specified network address.
    """

    data: dict[str, bytes]
    engine: Engine

    def __init__(self, engine: Engine) -> None:
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
            handle: The client handle.
            bulk_str: The buffer containing the data to be shared.
            bulk_size: The size of the data being transferred.
            key: The data key.
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
            handle: The client handle.
            bulk_str: The buffer that will store shared data.
            bulk_size: The size of the data to be received.
            key: The data's key.
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
            handle: The client handle.
            bulk_str: The buffer that will store shared data.
            bulk_size: The size of the data to be received.
            key: The data's key.
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
            handle: The client handle.
            bulk_str: The buffer that will store shared data.
            bulk_size: The size of the data to be received.
            key: The data's key.
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
