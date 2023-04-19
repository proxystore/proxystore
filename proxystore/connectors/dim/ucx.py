"""UCX-based distributed in-memory connector implementation."""
from __future__ import annotations

import asyncio
import atexit
import logging
import multiprocessing
import signal
import sys
import uuid
from time import sleep
from types import TracebackType
from typing import Any
from typing import NamedTuple
from typing import Sequence

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

try:
    import ucp

    ucx_import_error = None
except ImportError as e:  # pragma: no cover
    ucx_import_error = e

from proxystore.connectors.dim.exceptions import ServerTimeoutError
from proxystore.connectors.dim.rpc import RPC
from proxystore.connectors.dim.rpc import RPCResponse
from proxystore.connectors.dim.utils import get_ip_address
from proxystore.serialize import deserialize
from proxystore.serialize import serialize

ENCODING = 'UTF-8'
MAX_CHUNK_LENGTH_DEFAULT = 64 * 1024

server_process = None
logger = logging.getLogger(__name__)


class UCXKey(NamedTuple):
    """Key to objects stored across `UCXConnector`s."""

    key: str
    """Unique object key."""
    size: int
    """Object size in bytes."""
    peer: str
    """Peer where object is located."""


class UCXConnector:
    """UCX-based distributed in-memory connector.

    Note:
        The first instance of this connector created on a process will
        spawn a [`UCXServer`][proxystore.connectors.dim.ucx.UCXServer]
        that will store data. Hence, this connector just acts as an interface
        to that server.

    Args:
        interface: The network interface to use.
        port: The desired port for the spawned server.
        chunk_length: Message chunk size in bytes. Defaults to
            `MAX_CHUNK_LENGTH_DEFAULT`.
        timeout: Timeout in seconds to try connecting to local server before
            spawning one.

    Raises:
        ServerTimeoutError: If a local server cannot be connected to within
            `timeout` seconds, and a new local server does not response within
            `timeout` seconds after being started.
    """

    addr: str
    host: str
    port: int
    server: multiprocessing.Process
    _loop: asyncio.events.AbstractEventLoop

    # TODO : make host optional and try to get infiniband path automatically
    def __init__(
        self,
        interface: str,
        port: int,
        chunk_length: int | None = None,
        timeout: float = 1,
    ) -> None:
        if ucx_import_error is not None:  # pragma: no cover
            raise ucx_import_error

        logger.debug('Instantiating client and server')

        self.interface = interface
        self.port = port
        self.chunk_length = (
            MAX_CHUNK_LENGTH_DEFAULT if chunk_length is None else chunk_length
        )
        self.timeout = timeout

        self.host = get_ip_address(interface)
        self.addr = f'{self.host}:{self.port}'

        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.new_event_loop()

        self.server = multiprocessing.Process | None
        try:
            logger.info(
                f'Connecting to local server (address={self.addr})...',
            )
            wait_for_server(self.host, self.port, self.timeout)
            logger.info(
                f'Connected to local server (address={self.addr})',
            )
        except ServerTimeoutError:
            logger.info(
                'Failed to connect to local server '
                f'(address={self.addr}, timeout={self.timeout})',
            )
            self.server = spawn_server(
                self.host,
                self.port,
                chunk_length=self.chunk_length,
                spawn_timeout=self.timeout,
            )
            logger.info(f'Spawned local server (address={self.addr})')
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

    async def _send_rpcs_async(self, rpcs: Sequence[RPC]) -> list[RPCResponse]:
        responses = []

        ep = await ucp.create_endpoint(self.host, self.port)

        for rpc in rpcs:
            message = serialize(rpc)
            await ep.send_obj(message)
            logger.debug(
                f'Sent {rpc.operation.upper()} RPC '
                f'(key={rpc.key}, server={self.addr})',
            )
            response = await deserialize(ep.recv_obj())

            logger.debug(
                f'Received {rpc.operation.upper()} RPC response '
                f'(key={response.key}, server={self.addr}, '
                f'exception={response.exception is not None})',
            )

            if response.exception is not None:
                raise response.exception

            assert rpc.operation == response.operation
            assert rpc.key == response.key

            responses.append(response)

        await ep.close()

        return responses

    def _send_rpcs(self, rpcs: Sequence[RPC]) -> list[RPCResponse]:
        """Send an RPC request to the server.

        Args:
            rpcs: List of RPCs to invoke on local server.

        Returns:
            List of RPC responses.

        Raises:
            Exception: Any exception returned by the local server.
        """
        return self._loop.run_until_complete(self._send_rpcs_async(rpcs))

    def close(self, kill_server: bool = False) -> None:
        """Close the connector.

        Args:
            kill_server: Whether to kill the server process. If this instance
                did not spawn the local node's server process, this is a
                no-op.
        """
        logger.info('Clean up requested')

        if kill_server and self.server is not None:
            self.server.terminate()
            self.server.join()
            logger.info(
                'Terminated local server on connector close '
                f'(pid={self.server.pid})',
            )

        logger.debug('Clean up completed')

    def config(self) -> dict[str, Any]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        return {
            'interface': self.interface,
            'port': self.port,
            'chunk_length': self.chunk_length,
            'timeout': self.timeout,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> UCXConnector:
        """Create a new connector instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.
        """
        return cls(**config)

    def evict(self, key: UCXKey) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        logger.debug(f'Client issuing an evict request on key {key}.')

        rpc = RPC(operation='evict', key=key.key, size=key.size)
        self._loop.run_until_complete(self._send_rpcs([rpc]))

    def exists(self, key: UCXKey) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        rpc = RPC(operation='exists', key=key.key, size=key.size)
        (response,) = self._loop.run_until_complete(self._send_rpcs([rpc]))
        assert response.exists is not None
        return response.exists

    def get(self, key: UCXKey) -> bytes | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        rpc = RPC(operation='get', key=key.key, size=key.size)
        (result,) = self._loop.run_until_complete(self._send_rpcs([rpc]))
        return result.data

    def get_batch(self, keys: Sequence[UCXKey]) -> list[bytes | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or \
            `None` if the corresponding key does not have an associated object.
        """
        rpcs = [
            RPC(operation='get', key=key.key, size=key.size) for key in keys
        ]
        responses = self._loop.run_until_complete(self._send_rpcs(rpcs))
        return [r.data for r in responses]

    def put(self, obj: bytes) -> UCXKey:
        """Put a serialized object in the store.

        Args:
            obj: Serialized object to put in the store.

        Returns:
            Key which can be used to retrieve the object.
        """
        key = UCXKey(
            ucx_key=str(uuid.uuid4()),
            obj_size=len(obj),
            peer=self.addr,
        )
        logger.debug(
            f'Client issuing set request on key {key} with addr {self.addr}',
        )
        rpc = RPC(operation='put', key=key.key, size=key.size, data=obj)
        self._loop.run_until_complete(self._send_rpcs([rpc]))
        return key

    def put_batch(self, objs: Sequence[bytes]) -> list[UCXKey]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.

        Returns:
            List of keys with the same order as `objs` which can be used to \
            retrieve the objects.
        """
        keys = [
            UCXKey(key=str(uuid.uuid4()), size=len(obj), peer=self.addr)
            for obj in objs
        ]
        rpcs = [
            RPC(operation='put', key=key.key, size=key.size, data=obj)
            for key, obj in zip(keys, objs)
        ]
        self._loop.run_until_complete(self._send_rpcs(rpcs))
        return keys


class UCXServer:
    """UCXServer implementation.

    Args:
        host: The server host.
        port: The server port.
    """

    def __init__(self) -> None:
        self.data: dict[str, bytes] = {}

    def evict(self, key: str) -> None:
        """Remove key from local dictionary.

        Args:
            key: The object to evict's key.

        Returns:
            Operation status.
        """
        self.data.pop(key, None)

    def exists(self, key: str) -> bool:
        """Check if a key exists within local dictionary.

        Args:
            key: The object's key.

        Returns:
            If the object exists.
        """
        return key in self.data

    def get(self, key: str) -> bytes | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Data or `None` if no data associated with the key exists.
        """
        return self.data.get(key, None)

    def put(self, key: str, data: bytes) -> None:
        """Put data in the store.

        Args:
            key: Key associated with data.
            data: Data to put in the store.
        """
        self.data[key] = data

    async def handle_rpc(self, rpc: RPC) -> RPCResponse:
        """Process an RPC request.

        Args:
            rpc: Client RPC to process.

        Returns:
            Response containing result or an exception if the operation failed.
        """
        response: RPCResponse
        try:
            if rpc.operation == 'exists':
                exists = self.exists(rpc.key)
                response = RPCResponse(
                    'exists',
                    key=rpc.key,
                    size=rpc.size,
                    exists=exists,
                )
            elif rpc.operation == 'evict':
                self.evict(rpc.key)
                response = RPCResponse('evict', key=rpc.key, size=rpc.size)
            elif rpc.operation == 'get':
                data = self.get(rpc.key)
                response = RPCResponse(
                    'get',
                    key=rpc.key,
                    size=rpc.size,
                    data=data,
                )
            elif rpc.operation == 'put':
                assert rpc.data is not None
                self.put(rpc.key, rpc.data)
                response = RPCResponse('put', key=rpc.key, size=rpc.size)
            else:
                raise AssertionError('Unreachable.')
        except Exception as e:
            response = RPCResponse(
                rpc.operation,
                key=rpc.key,
                size=rpc.size,
                exception=e,
            )
        return response

    def close(self) -> None:
        """Close the server."""
        if self.ucp_listener is not None:
            self.ucp_listener.close()

            while not self.ucp_listener.closed():
                sleep(0.001)

            # Need to lose reference to Listener because UCP does reference
            # counting
            del self.ucp_listener
            self.ucp_listener = None


def reset_ucp() -> None:  # pragma: no cover
    """Hard reset all of UCP.

    UCP provides `ucp.reset()`; however, this function does not correctly
    shutdown all asyncio tasks and readers. This function wraps
    `ucp.reset()` and additionally removes all readers on the event loop
    and cancels/awaits all asyncio tasks.
    """

    def inner_context() -> None:
        ctx = ucp.core._get_ctx()

        for task in ctx.progress_tasks:
            if task is None:
                continue
            task.event_loop.remove_reader(ctx.epoll_fd)
            if task.asyncio_task is not None:
                try:
                    task.asyncio_task.cancel()
                    task.event_loop.run_until_complete(task.asyncio_task)
                except asyncio.CancelledError:
                    pass

    # We access ucp.core._get_ctx() inside this nested function so our local
    # reference to the UCP context goes out of scope before calling
    # ucp.reset(). ucp.reset() will fail if there are any weak references to
    # to the UCP context because it assumes those may be Listeners or
    # Endpoints that were not properly closed.
    inner_context()

    try:
        ucp.reset()
    except ucp.UCXError:
        pass


async def reset_ucp_async() -> None:  # pragma: no cover
    """Hard reset all of UCP.

    UCP provides `ucp.reset()`; however, this function does not correctly
    shutdown all asyncio tasks and readers. This function wraps
    `ucp.reset()` and additionally removes all readers on the event loop
    and cancels/awaits all asyncio tasks.
    """

    async def inner_context() -> None:
        ctx = ucp.core._get_ctx()

        for task in ctx.progress_tasks:
            if task is None:
                continue
            task.event_loop.remove_reader(ctx.epoll_fd)
            if task.asyncio_task is not None:
                try:
                    task.asyncio_task.cancel()
                    await task.asyncio_task
                except asyncio.CancelledError:
                    pass

    # We access ucp.core._get_ctx() inside this nested function so our local
    # reference to the UCP context goes out of scope before calling
    # ucp.reset(). ucp.reset() will fail if there are any weak references to
    # to the UCP context because it assumes those may be Listeners or
    # Endpoints that were not properly closed.
    await inner_context()

    try:
        ucp.reset()
    except ucp.UCXError:
        pass


async def run_server(
    host: str,
    port: int,
    chunk_length: int | None = None,
) -> None:
    """Listen and reply to RPCs from clients.

    Warning:
        This function does not return until SIGINT or SIGTERM is received.

    Args:
        host: IP address the server should bind to.
        port: Port the server should listen on.
        chunk_length: Message chunk size in bytes. Defaults to
            `MAX_CHUNK_LENGTH_DEFAULT`.
    """
    loop = asyncio.get_running_loop()
    close_future = loop.create_future()
    loop.add_signal_handler(signal.SIGINT, close_future.set_result, None)
    loop.add_signal_handler(signal.SIGTERM, close_future.set_result, None)

    server = UCXServer()
    chunk_length = (
        MAX_CHUNK_LENGTH_DEFAULT if chunk_length is None else chunk_length
    )

    ucp_listener = ucp.create_listener(server.handle_rpc, port)

    await close_future
    ucp_listener.close()

    while not ucp_listener.closed():
        sleep(0.001)
    await reset_ucp_async()


def start_server(
    host: str,
    port: int,
    chunk_length: int | None = None,
) -> None:
    """Run a local server.

    Note:
        This function creates an event loop and executes
        [`run_server()`][proxystore.connectors.dim.ucx.run_server] within
        that loop.

    Args:
        host: IP address the server should bind to.
        port: Port the server should listen on.
        chunk_length: Message chunk size in bytes. Defaults to
            `MAX_CHUNK_LENGTH_DEFAULT`.
    """
    asyncio.run(run_server(host, port, chunk_length))


def spawn_server(
    host: str,
    port: int,
    *,
    chunk_length: int | None = None,
    spawn_timeout: float = 5.0,
    kill_timeout: float | None = 1.0,
) -> multiprocessing.Process:
    """Spawn a local server running in a separate process.

    Note:
        An `atexit` callback is registered which will terminate the spawned
        server process when the calling process exits.

    Args:
        host: The host of the server to ping.
        port: The port of the server to ping.
        chunk_length: Message chunk size in bytes. Defaults to
            `MAX_CHUNK_LENGTH_DEFAULT`.
        spawn_timeout: Max time in seconds to wait for the server to start.
        kill_timeout: Max time in seconds to wait for the server to shutdown
            on exit.

    Returns:
        The process that the server is running in.
    """
    server_process = multiprocessing.Process(
        target=start_server,
        args=(host, port, chunk_length),
    )
    server_process.start()

    def _kill_on_exit() -> None:  # pragma: no cover
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

    wait_for_server(host, port, timeout=spawn_timeout)
    logger.debug(
        f'Server started (host={host}, port={port}, pid={server_process.pid})',
    )

    return server_process


async def wait_for_server(host: str, port: int, timeout: float = 0.1) -> None:
    """Wait until the UCXServer responds.

    Args:
        host: The host of UCXServer to ping.
        port: Theport of UCXServer to ping.
        timeout: The max time in seconds to wait for server response.

    Raises:
        ServerTimeoutError: If the server does not respond within the timeout.
    """
    sleep_time = 0.01
    time_waited = 0.0

    while True:
        try:
            ep = await ucp.create_endpoint(host, port)
        except ucp._libs.exceptions.UCXNotConnected as e:  # pragma: no cover
            if time_waited >= timeout:
                raise RuntimeError(
                    'Failed to connect to server within timeout '
                    f'({timeout} seconds).',
                ) from e
            await asyncio.sleep(sleep_time)
            time_waited += sleep_time
        else:
            break  # pragma: no cover

    await ep.send_obj(bytes(1))
    _ = await ep.recv_obj()
    await ep.close()
    assert ep.closed()
