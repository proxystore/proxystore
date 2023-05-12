"""UCX-based distributed in-memory connector implementation."""
from __future__ import annotations

import asyncio
import atexit
import logging
import multiprocessing
import signal
import sys
import uuid
from types import TracebackType
from typing import Any
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
from proxystore.connectors.dim.models import DIMKey
from proxystore.connectors.dim.models import RPC
from proxystore.connectors.dim.models import RPCResponse
from proxystore.serialize import deserialize
from proxystore.serialize import serialize

logger = logging.getLogger(__name__)


class UCXConnector:
    """UCX-based distributed in-memory connector.

    Note:
        The first instance of this connector created on a process will
        spawn a [`UCXServer`][proxystore.connectors.dim.ucx.UCXServer]
        that will store data. Hence, this connector just acts as an interface
        to that server.

    Args:
        port: The desired port for the spawned server.
        address: The IP address of the network interface to use.
            Has precedence over `interface` if both are provided.
        interface: The network interface to use.
            `address` has precedence if both args are defined.
        timeout: Timeout in seconds to try connecting to local server before
            spawning one.

    Raises:
        ServerTimeoutError: If a local server cannot be connected to within
            `timeout` seconds, and a new local server does not response within
            `timeout` seconds after being started.
    """

    def __init__(
        self,
        port: int,
        address: str | None = None,
        interface: str | None = None,
        timeout: float = 1,
    ) -> None:
        if ucx_import_error is not None:  # pragma: no cover
            raise ucx_import_error

        self._address = address
        self._interface = interface
        self.port = port
        self.timeout = timeout

        if self._address is not None:
            self.address = self._address
        elif self._interface is not None:
            self.address = ucp.get_address(ifname=self._interface)
        else:
            self.address = ucp.get_address()

        self.url = f'{self.address}:{self.port}'

        self.server: multiprocessing.context.SpawnProcess | None
        try:
            logger.info(
                f'Connecting to local server (URL={self.url})...',
            )
            wait_for_server(self.address, self.port, self.timeout)
            logger.info(
                f'Connected to local server (URL={self.url})',
            )
        except ServerTimeoutError:
            logger.info(
                'Failed to connect to local server '
                f'(URL={self.url}, timeout={self.timeout})',
            )
            self.server = spawn_server(
                self.address,
                self.port,
                spawn_timeout=self.timeout,
            )
            logger.info(f'Spawned local server (address={self.url})')
        else:
            self.server = None

        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.new_event_loop()

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

        for rpc in rpcs:
            ep = await ucp.create_endpoint(
                rpc.key.peer_host,
                rpc.key.peer_port,
            )

            message = serialize(rpc)
            await ep.send_obj(message)
            logger.debug(
                f'Sent {rpc.operation.upper()} RPC (key={rpc.key})',
            )
            response = deserialize(bytes(await ep.recv_obj()))

            logger.debug(
                f'Received {rpc.operation.upper()} RPC response '
                f'(key={response.key}, '
                'exception={response.exception is not None})',
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
        if kill_server and self.server is not None:
            self.server.terminate()
            self.server.join()
            logger.info(
                'Terminated local server on connector close '
                f'(pid={self.server.pid})',
            )

        logger.debug('Closed UCX connector')

    def config(self) -> dict[str, Any]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        return {
            'address': self._address,
            'interface': self._interface,
            'port': self.port,
            'timeout': self.timeout,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> UCXConnector:
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
        rpc = RPC(operation='get', key=key)
        (result,) = self._send_rpcs([rpc])
        return result.data

    def get_batch(self, keys: Sequence[DIMKey]) -> list[bytes | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or \
            `None` if the corresponding key does not have an associated object.
        """
        rpcs = [RPC(operation='get', key=key) for key in keys]
        responses = self._send_rpcs(rpcs)
        return [r.data for r in responses]

    def put(self, obj: bytes) -> DIMKey:
        """Put a serialized object in the store.

        Args:
            obj: Serialized object to put in the store.

        Returns:
            Key which can be used to retrieve the object.
        """
        key = DIMKey(
            dim_type='ucx',
            obj_id=str(uuid.uuid4()),
            size=len(obj),
            peer_host=self.address,
            peer_port=self.port,
        )
        rpc = RPC(operation='put', key=key, data=obj)
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
                dim_type='ucx',
                obj_id=str(uuid.uuid4()),
                size=len(obj),
                peer_host=self.address,
                peer_port=self.port,
            )
            for obj in objs
        ]
        rpcs = [
            RPC(operation='put', key=key, data=obj)
            for key, obj in zip(keys, objs)
        ]
        self._send_rpcs(rpcs)
        return keys


class UCXServer:
    """UCXServer implementation."""

    def __init__(self) -> None:
        self.data: dict[str, bytes] = {}

    def evict(self, key: str) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        self.data.pop(key, None)

    def exists(self, key: str) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
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

    def handle_rpc(self, rpc: RPC) -> RPCResponse:
        """Process an RPC request.

        Args:
            rpc: Client RPC to process.

        Returns:
            Response containing result or an exception if the operation failed.
        """
        response: RPCResponse

        try:
            if rpc.operation == 'exists':
                exists = self.exists(rpc.key.obj_id)
                response = RPCResponse('exists', key=rpc.key, exists=exists)
            elif rpc.operation == 'evict':
                self.evict(rpc.key.obj_id)
                response = RPCResponse('evict', key=rpc.key)
            elif rpc.operation == 'get':
                data = self.get(rpc.key.obj_id)
                response = RPCResponse('get', key=rpc.key, data=data)
            elif rpc.operation == 'put':
                assert rpc.data is not None
                self.put(rpc.key.obj_id, rpc.data)
                response = RPCResponse('put', key=rpc.key)
            else:
                raise AssertionError('Unreachable.')
        except Exception as e:
            response = RPCResponse(rpc.operation, key=rpc.key, exception=e)
        return response

    async def handler(self, ep: ucp.Endpoint) -> None:
        """Handle endpoint requests.

        Args:
            ep: The endpoint making the request.
        """
        rpc_bytes = bytes(await ep.recv_obj())

        if rpc_bytes == b'ping':
            await ep.send_obj(b'pong')
            return

        rpc: RPC = deserialize(rpc_bytes)
        response = self.handle_rpc(rpc)

        message = serialize(response)
        await ep.send_obj(message)


async def run_server(port: int) -> None:  # pragma: no cover
    """Listen and reply to RPCs from clients.

    Warning:
        This function does not return until SIGINT or SIGTERM is received.

    Args:
        port: Port the server should listen on.
    """
    server = UCXServer()
    ucp_listener = ucp.create_listener(server.handler, port)

    loop = asyncio.get_running_loop()
    close_future = loop.create_future()
    loop.add_signal_handler(signal.SIGINT, close_future.set_result, None)
    loop.add_signal_handler(signal.SIGTERM, close_future.set_result, None)

    await close_future
    ucp_listener.close()

    while not ucp_listener.closed():
        await asyncio.sleep(0.001)

    loop.remove_signal_handler(signal.SIGINT)
    loop.remove_signal_handler(signal.SIGTERM)

    # UCP does reference counting of open resources
    del ucp_listener
    await reset_ucp_async()


def start_server(port: int) -> None:  # pragma: no cover
    """Run a local server.

    Note:
        This function creates an event loop and executes
        [`run_server()`][proxystore.connectors.dim.ucx.run_server] within
        that loop.

    Args:
        port: Port the server should listen on.
    """
    asyncio.run(run_server(port))


def spawn_server(
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
        address: IP address the server will listen on.
        port: Port the server will listen on.
        spawn_timeout: Max time in seconds to wait for the server to start.
        kill_timeout: Max time in seconds to wait for the server to shutdown
            on exit.

    Returns:
        The process that the server is running in.
    """
    ctx = multiprocessing.get_context('spawn')
    # UCX seems to hang if you fork a process after calling ucp.init().
    # If discovered this via a comment in Dask's distributed communication:
    # https://github.com/dask/distributed/blob/76bbfaf9f4a14906cbf4500ed42c442c7a5bc971/distributed/comm/ucx.py#L40  # noqa: E501
    server_process = ctx.Process(
        target=start_server,
        args=(port,),
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

    wait_for_server(address, port, timeout=spawn_timeout)
    logger.debug(
        f'Server started (host={address}, port={port}, pid={server_process.pid})',
    )

    return server_process


async def wait_for_server_async(
    address: str,
    port: int,
    timeout: float = 0.1,
) -> None:
    """Wait until the server responds.

    Args:
        address: Host IP of the server to ping.
        port: Port of the server to ping.
        timeout: Max time in seconds to wait for server response.

    Raises:
        ServerTimeoutError: If the server does not respond within the timeout.
    """
    sleep_time = 0.01
    time_waited = 0.0

    while True:
        try:
            ep = await ucp.create_endpoint(address, port)
        except ucp._libs.exceptions.UCXNotConnected as e:  # pragma: no cover
            if time_waited >= timeout:
                raise ServerTimeoutError(
                    'Failed to connect to server within timeout '
                    f'({timeout} seconds).',
                ) from e
            await asyncio.sleep(sleep_time)
            time_waited += sleep_time
        else:
            break  # pragma: no cover

    await ep.send_obj(b'ping')
    assert bytes(await ep.recv_obj()) == b'pong'
    await ep.close()
    assert ep.closed()
    del ep


def wait_for_server(address: str, port: int, timeout: float = 0.1) -> None:
    """Wait until the server responds.

    Note:
        This function calls
        [`wait_for_server_async()`][proxystore.connectors.dim.ucx.wait_for_server_async]
        using [`asyncio.run()`][asyncio.run].

    Args:
        address: The host IP of the server to ping.
        port: Theport of the server to ping.
        timeout: The max time in seconds to wait for server response.

    Raises:
        ServerTimeoutError: If the server does not respond within the timeout.
    """
    asyncio.run(wait_for_server_async(address, port, timeout))


async def reset_ucp_async(reset_ucp: bool = True) -> None:  # pragma: no cover
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
                # A RuntimeError can happen if the task if from a different
                # event loop. We'll just skip these for now
                except (asyncio.CancelledError, RuntimeError):
                    pass

    # We access ucp.core._get_ctx() inside this nested function so our local
    # reference to the UCP context goes out of scope before calling
    # ucp.reset(). ucp.reset() will fail if there are any weak references to
    # to the UCP context because it assumes those may be Listeners or
    # Endpoints that were not properly closed.
    await inner_context()

    if reset_ucp:
        try:
            ucp.reset()
        except ucp.UCXError:
            pass
