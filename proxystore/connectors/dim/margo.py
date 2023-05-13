"""Margo RPC-based distributed in-memory connector implementation."""
from __future__ import annotations

import atexit
import logging
import multiprocessing
import os
import sys
import time
import uuid
from enum import Enum
from types import TracebackType
from typing import Any
from typing import Sequence

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

try:
    import pymargo
    import pymargo.bulk as bulk
    from pymargo.bulk import Bulk
    from pymargo.core import Engine
    from pymargo.core import Handle
    from pymargo.core import MargoException

    pymargo_import_error = None
except ImportError as e:  # pragma: no cover
    pymargo_import_error = e

from proxystore.connectors.dim.exceptions import ServerTimeoutError
from proxystore.connectors.dim.models import DIMKey
from proxystore.connectors.dim.models import RPC
from proxystore.connectors.dim.models import RPCResponse
from proxystore.connectors.dim.utils import get_ip_address
from proxystore.serialize import deserialize
from proxystore.serialize import serialize

logger = logging.getLogger(__name__)


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


class MargoConnector:
    """Margo RPC-based distributed in-memory connector.

    Note:
        The first instance of this connector created on a process will
        spawn a [`MargoServer`][proxystore.connectors.dim.margo.MargoServer]
        that will store data. Hence, this connector just acts as an interface
        to that server.

    Args:
        port: The desired port for the spawned server.
        protocol: The communication protocol to use.
        address: The network IP to use for transfer. Has precedence over `interface`
            if both are provided.
        interface: The network interface to use. `addr` has precedence over
            this attribute if both are provided.
        timeout: Timeout in seconds to try connecting to a local server before
            spawning one.
        force_spawn_server: Force spawn a server rather than waiting to check
            if one is already running.

    Raises:
        ServerTimeoutError: If a local server cannot be connected to within
            `timeout` seconds, and a new local server does not respond within
            `timeout` seconds after being started.
    """

    def __init__(
        self,
        port: int,
        protocol: Protocol | str,
        address: str | None = None,
        interface: str | None = None,
        timeout: float = 1,
        force_spawn_server: bool = False,
    ) -> None:
        # Py-Mochi-Margo is not a required dependency and requires the user
        # to install it themselves.
        if pymargo_import_error is not None:  # pragma: no cover
            raise pymargo_import_error

        self._address = address
        self._interface = interface
        self.port = port
        self.protocol = (
            protocol if isinstance(protocol, str) else protocol.value
        )

        self.timeout = timeout
        self.force_spawn_server = force_spawn_server

        self.engine = Engine(
            self.protocol,
            mode=pymargo.client,
            use_progress_thread=True,
        )

        if self._address is not None:
            self.address = self._address
        elif self._interface is not None:  # pragma: darwin no cover
            self.address = get_ip_address(self._interface)
        else:
            eng_url = str(self.engine.addr())
            self.address = eng_url.split(':')[1].split('/')[2]

        self.url = f'{self.protocol}://{self.address}:{self.port}'

        self._rpcs = {
            'evict': self.engine.register('evict'),
            'exists': self.engine.register('exists'),
            'get': self.engine.register('get'),
            'put': self.engine.register('put'),
        }

        server_available = False
        if not force_spawn_server:
            try:
                logger.info(
                    f'Connecting to local server (address={self.url})...',
                )
                wait_for_server(
                    self.protocol,
                    self.address,
                    self.port,
                    self.timeout,
                )
                logger.info(
                    f'Connected to local server (address={self.url})',
                )
                server_available = True
            except ServerTimeoutError:
                logger.info(
                    'Failed to connect to local server '
                    f'(address={self.url}, timeout={self.timeout})',
                )

        self.server: multiprocessing.context.SpawnProcess | None
        if not server_available or force_spawn_server:
            self.server = spawn_server(
                self.protocol,
                self.address,
                self.port,
                spawn_timeout=self.timeout,
            )
            logger.info(f'Spawned local server (address={self.url})')
        else:
            self.server = None

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _send_rpcs(self, rpcs: Sequence[RPC]) -> list[RPCResponse]:
        """Send an RPC request to the server.

        Args:
            rpcs: List of RPCs to invoke on local server.

        Returns:
            List of RPC responses.

        Raises:
            Exception: Any exception returned by the local server.
        """
        responses = []

        for rpc in rpcs:
            url = f'{self.protocol}://{rpc.key.peer_host}:{rpc.key.peer_port}'
            server_url = self.engine.lookup(url)
            logger.debug(
                f'Sent {rpc.operation.upper()} RPC (key={rpc.key})',
            )
            result = self._rpcs[rpc.operation].on(server_url)(
                rpc.data,
                rpc.key.size,
                rpc.key,
            )

            response = deserialize(result)
            logger.debug(
                f'Received {rpc.operation.upper()} RPC response '
                f'(key={response.key}, '
                f'exception={response.exception is not None})',
            )

            if response.exception is not None:
                raise response.exception

            assert rpc.operation == response.operation
            assert rpc.key == response.key

            responses.append(response)

        return responses

    def close(self, kill_server: bool = True) -> None:
        """Close the connector.

        Args:
            kill_server: Whether to kill the server process. If this instance
                did not spawn the local node's server process, this is a
                no-op.
        """
        if kill_server and self.server is not None:
            self.engine.lookup(self.url).shutdown()
            self.server.join()
            logger.info(
                'Terminated local server on connector close '
                f'(pid={self.server.pid})',
            )

        self.engine.finalize()
        logger.info('Closed Margo connector')

    def config(self) -> dict[str, Any]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        return {
            'address': self._address,
            'interface': self._interface,
            'port': self.port,
            'protocol': self.protocol,
            'timeout': self.timeout,
            'force_spawn_server': self.force_spawn_server,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> MargoConnector:
        """Create a new connector instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.
        """
        return cls(**config)

    def evict(self, key: DIMKey) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        rpc = RPC(operation='evict', key=key)
        self._send_rpcs([rpc])

    def exists(self, key: DIMKey) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        rpc = RPC(operation='exists', key=key)
        (response,) = self._send_rpcs([rpc])
        assert response.exists is not None
        return response.exists

    def get(self, key: DIMKey) -> bytes | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        buff = bytearray(key.size)
        blk = self.engine.create_bulk(buff, bulk.write_only)

        rpc = RPC(operation='get', key=key, data=blk)
        (result,) = self._send_rpcs([rpc])

        if result.exists:
            return bytes(buff)

        return None

    def get_batch(self, keys: Sequence[DIMKey]) -> list[bytes | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or \
            `None` if the corresponding key does not have an associated object.
        """
        rpcs: list[RPC] = []
        buffers: list[bytearray] = []

        for key in keys:
            buff = bytearray(key.size)
            blk = self.engine.create_bulk(buff, bulk.write_only)

            buffers.append(buff)
            rpcs.append(RPC(operation='get', key=key, data=blk))

        responses = self._send_rpcs(rpcs)
        return [
            bytes(b) if responses[i].exists else None
            for i, b in enumerate(buffers)
        ]

    def put(self, obj: bytes) -> DIMKey:
        """Put a serialized object in the store.

        Args:
            obj: Serialized object to put in the store.

        Returns:
            Key which can be used to retrieve the object.
        """
        key = DIMKey(
            dim_type='margo',
            obj_id=str(uuid.uuid4()),
            size=len(obj),
            peer_host=self.address,
            peer_port=self.port,
        )
        blk = self.engine.create_bulk(obj, bulk.read_only)

        rpc = RPC(operation='put', key=key, data=blk)
        self._send_rpcs([rpc])
        return key

    def put_batch(self, objs: Sequence[bytes]) -> list[DIMKey]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.

        Returns:
            List of keys with the same order as `objs` which can be used to \
            retrieve the objects.
        """
        keys = [
            DIMKey(
                dim_type='margo',
                obj_id=str(uuid.uuid4()),
                size=len(obj),
                peer_host=self.address,
                peer_port=self.port,
            )
            for obj in objs
        ]
        rpcs: list[RPC] = []

        for key, obj in zip(keys, objs):
            blk = self.engine.create_bulk(obj, bulk.read_only)
            rpcs.append(RPC(operation='put', key=key, data=blk))

        self._send_rpcs(rpcs)

        return keys


class MargoServer:
    """MargoServer implementation."""

    def __init__(self, engine: Engine) -> None:
        self.data: dict[str, bytes] = {}
        self.engine = engine

    def evict(
        self,
        handle: Handle,
        bulk_str: Bulk,
        bulk_size: int,
        key: DIMKey,
    ) -> None:
        """Remove key from local dictionary.

        Args:
            handle: The client handle.
            bulk_str: The buffer that will store shared data.
            bulk_size: The size of the data to be received.
            key: The data's key.
        """
        self.data.pop(key.obj_id, None)
        response = RPCResponse(operation='evict', key=key)
        handle.respond(serialize(response))

    def exists(
        self,
        handle: Handle,
        bulk_str: Bulk,
        bulk_size: int,
        key: DIMKey,
    ) -> None:
        """Check if key exists within local dictionary.

        Args:
            handle: The client handle.
            bulk_str: The buffer that will store shared data.
            bulk_size: The size of the data to be received.
            key: The data's key.
        """
        exists = key.obj_id in self.data
        response = RPCResponse(operation='exists', key=key, exists=exists)
        handle.respond(serialize(response))

    def get(
        self,
        handle: Handle,
        bulk_str: Bulk,
        bulk_size: int,
        key: DIMKey,
    ) -> None:
        """Return data at a given key back to the client.

        Args:
            handle: The client handle.
            bulk_str: The buffer that will store shared data.
            bulk_size: The size of the data to be received.
            key: The data's key.
        """
        local_array = self.data.get(key.obj_id, None)
        if local_array is not None:
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
            response = RPCResponse(operation='get', key=key, exists=True)
        else:
            response = RPCResponse(operation='get', key=key, exists=False)
        handle.respond(serialize(response))

    def put(
        self,
        handle: Handle,
        bulk_str: Bulk,
        bulk_size: int,
        key: DIMKey,
    ) -> None:
        """Obtain data from the client and store it in local dictionary.

        Args:
            handle: The client handle.
            bulk_str: The buffer containing the data to be shared.
            bulk_size: The size of the data being transferred.
            key: The data key.
        """
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
        self.data[key.obj_id] = local_buffer

        response = RPCResponse(operation='put', key=key)
        handle.respond(serialize(response))


def _when_finalize() -> None:
    logger.info(f'Margo server finalized (pid={os.getpid()})')


def start_server(url: str) -> None:
    """Start and wait on a Margo server.

    Args:
        url: URL of the engine that will be started. Should take
            the form `{protocol}://{host}:{port}`.
    """
    server_engine = Engine(url)
    server_engine.on_finalize(_when_finalize)
    server_engine.enable_remote_shutdown()

    receiver = MargoServer(server_engine)
    server_engine.register('evict', receiver.evict)
    server_engine.register('exists', receiver.exists)
    server_engine.register('get', receiver.get)
    server_engine.register('put', receiver.put)
    server_engine.wait_for_finalize()


def spawn_server(
    protocol: str,
    address: str,
    port: int,
    *,
    spawn_timeout: float = 5.0,
    kill_timeout: float | None = 1.0,
) -> multiprocessing.context.SpawnProcess:
    """Spawn a local server running in a separate process.

    Note:
        An `atexit` callback is registered which will terminate the spawned
        server process when the calling process exits.

    Args:
        protocol: Communication protocol.
        address: Host IP of the server to wait on.
        port: Port of the server to wait on.
        spawn_timeout: Max time in seconds to wait for the server to start.
        kill_timeout: Max time in seconds to wait for the server to shutdown
            on exit.

    Returns:
        The process that the server is running in.
    """
    url = f'{protocol}://{address}:{port}'

    ctx = multiprocessing.get_context('spawn')
    server_process = ctx.Process(
        target=start_server,
        args=(url,),
    )
    server_process.start()

    def _kill_on_exit() -> None:  # pragma: no cover
        if server_process.is_alive():
            server_process.terminate()
            server_process.join(timeout=kill_timeout)
            if server_process.is_alive():
                server_process.kill()
                server_process.join()
            logger.debug(
                'Server terminated on parent process exit '
                f'(pid={server_process.pid})',
            )

    atexit.register(_kill_on_exit)
    logger.debug('Registered server cleanup atexit callback')

    wait_for_server(protocol, address, port, timeout=spawn_timeout)
    logger.debug(
        f'Server started (address={url}, pid={server_process.pid})',
    )

    return server_process


def wait_for_server(
    protocol: str,
    address: str,
    port: int,
    timeout: float = 0.1,
) -> None:
    """Wait until the server responds.

    Warning:
        Due to how Margo blocks internally, the timeout is not very accurate.

    Args:
        protocol: Communication protocol.
        address: Host IP of the server to wait on.
        port: Port of the server to wait on.
        timeout: The max time in seconds to wait for server response.

    Raises:
        ServerTimeoutError: If the server does not respond within the timeout.
    """
    engine = Engine(protocol, mode=pymargo.client, use_progress_thread=True)
    remote_function = engine.register('exists')
    key = DIMKey(
        'margo',
        obj_id='ping',
        size=0,
        peer_host=address,
        peer_port=port,
    )
    rpc = RPC('exists', key=key)
    url = f'{protocol}://{address}:{port}'

    sleep_time = 0.01
    start = time.time()
    while time.time() - start < timeout:
        try:
            local_url = engine.lookup(url)
            result = remote_function.on(local_url)(
                rpc.data,
                rpc.key.size,
                rpc.key,
            )
            response = deserialize(result)
            assert response.exception is None
            # We could call engine.finalize() now to be safe but Margo
            # raises a _pymargo.MargoException: margo_addr_free() returned 11
            # exception.
            return
        except MargoException:  # pragma: no cover
            time.sleep(sleep_time)

    raise ServerTimeoutError(
        f'Failed to connect to server within timeout ({timeout} seconds).',
    )
