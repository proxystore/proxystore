"""UCX-based distributed in-memory connector implementation."""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
import uuid
from multiprocessing import Process
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

from proxystore.connectors.dim.utils import get_ip_address
from proxystore.connectors.dim.utils import Status
from proxystore.serialize import deserialize
from proxystore.serialize import SerializationError
from proxystore.serialize import serialize

ENCODING = 'UTF-8'

server_process = None
logger = logging.getLogger(__name__)


class UCXKey(NamedTuple):
    """Key to objects stored across `UCXConnector`s."""

    ucx_key: str
    """Unique object key."""
    obj_size: int
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
    """

    addr: str
    host: str
    port: int
    server: Process
    _loop: asyncio.events.AbstractEventLoop

    # TODO : make host optional and try to get infiniband path automatically
    def __init__(self, interface: str, port: int) -> None:
        global server_process

        if ucx_import_error is not None:  # pragma: no cover
            raise ucx_import_error

        logger.debug('Instantiating client and server')

        self.interface = interface
        self.host = get_ip_address(interface)
        self.port = port

        self.addr = f'{self.host}:{self.port}'

        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.new_event_loop()

        if server_process is None:
            server_process = Process(
                target=launch_server,
                args=(self.host, self.port),
            )
            server_process.start()
            self._loop.run_until_complete(
                wait_for_server(self.host, self.port),
            )

        # TODO: Verify if create_endpoint error handling will successfully
        # connect to endpoint or if error handling needs to be done here

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    async def handler(self, event: bytes, addr: str) -> bytes:
        """Handler that issues requests to the server."""
        host = addr.split(':')[0]  # quick fix
        port = int(addr.split(':')[1])

        ep = await ucp.create_endpoint(host, port)

        await ep.send_obj(event)

        res = await ep.recv_obj()

        await ep.close()

        return bytes(res)  # returns bytearray by default

    def close(self) -> None:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        global server_process

        logger.info('Clean up requested')

        if server_process is not None:
            server_process.terminate()
            server_process.join()
            server_process = None

        logger.debug('Clean up completed')

    def config(self) -> dict[str, Any]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        return {'interface': self.interface, 'port': self.port}

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

        event = serialize({'key': key.ucx_key, 'data': None, 'op': 'evict'})
        self._loop.run_until_complete(self.handler(event, key.peer))

    def exists(self, key: UCXKey) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        logger.debug(f'Client issuing an exists request on key {key}.')

        event = serialize(
            {'key': key.ucx_key, 'data': None, 'op': 'exists'},
        )
        return deserialize(
            self._loop.run_until_complete(self.handler(event, key.peer)),
        )

    def get(self, key: UCXKey) -> bytes | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        res: bytes | None
        logger.debug(f'Client issuing get request on key {key}.')

        event = serialize({'key': key.ucx_key, 'data': '', 'op': 'get'})
        res = self._loop.run_until_complete(self.handler(event, key.peer))

        try:
            s = deserialize(res)

            assert isinstance(s, Status)
            assert not s.success
            return None
        except SerializationError:
            return res

    def get_batch(self, keys: Sequence[UCXKey]) -> list[bytes | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or \
            `None` if the corresponding key does not have an associated object.
        """
        return [self.get(key) for key in keys]

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

        event = serialize({'key': key.ucx_key, 'data': obj, 'op': 'set'})

        self._loop.run_until_complete(self.handler(event, self.addr))
        return key

    def put_batch(self, objs: Sequence[bytes]) -> list[UCXKey]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.

        Returns:
            List of keys with the same order as `objs` which can be used to \
            retrieve the objects.
        """
        return [self.put(obj) for obj in objs]


class UCXServer:
    """UCXServer implementation.

    Args:
        host: The server host.
        port: The server port.
    """

    host: str
    port: int
    ucp_listener: ucp.core.Listener | None
    data: dict[str, bytes]

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.data = {}
        self.ucp_listener = None

    def set(self, key: str, data: bytes) -> Status:
        """Obtain data from the client and store it in local dictionary.

        Args:
            key: The object key to use.
            data: The data to store.

        Returns:
            Operation status.
        """
        self.data[key] = data
        return Status(success=True, error=None)

    def get(self, key: str) -> bytes | Status:
        """Return data at a given key back to the client.

        Args:
            key: The object key.

        Returns:
            Operation status.
        """
        try:
            return self.data[key]
        except KeyError as e:
            return Status(success=False, error=e)

    def evict(self, key: str) -> Status:
        """Remove key from local dictionary.

        Args:
            key: The object to evict's key.

        Returns:
            Operation status.
        """
        self.data.pop(key, None)
        return Status(success=True, error=None)

    def exists(self, key: str) -> bool:
        """Check if a key exists within local dictionary.

        Args:
            key: The object's key.

        Returns:
            If the object exists.
        """
        return key in self.data

    async def handler(self, ep: ucp.Endpoint) -> None:
        """Handle endpoint requests.

        Args:
            ep: The endpoint to communicate with.
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

    async def run(self) -> None:
        """Run this UCXServer forever.

        Creates a listener for the handler method and waits on SIGINT/TERM
        events to exit. Also handles cleaning up UCP objects.
        """
        self.ucp_listener = ucp.create_listener(self.handler, self.port)

        # Set the stop condition when receiving SIGINT (ctrl-C) and SIGTERM.
        loop = asyncio.get_running_loop()
        stop = loop.create_future()
        loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
        loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)

        await stop
        self.close()
        await reset_ucp_async()

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


def launch_server(host: str, port: int) -> None:
    """Launch the UCXServer in asyncio.

    Args:
        host: The host for server to listen on.
        port: The port for server to listen on.
    """
    logger.info(f'starting server on host {host} with port {port}')

    ps = UCXServer(host, port)
    # CI occasionally timeouts when starting this server in the
    # store_implementation session fixture. It seems to not happen when
    # debug=True, but this is just a temporary fix.
    asyncio.run(ps.run(), debug=True)

    logger.info(f'server running at address {host}:{port}')


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


async def wait_for_server(host: str, port: int, timeout: float = 5.0) -> None:
    """Wait until the UCXServer responds.

    Args:
        host: The host of UCXServer to ping.
        port: Theport of UCXServer to ping.
        timeout: The max time in seconds to wait for server response.
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
