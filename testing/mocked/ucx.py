"""UCX mocker implementation."""
from __future__ import annotations

from typing import Any

from proxystore.connectors.dim.models import RPC
from proxystore.connectors.dim.models import RPCResponse
from proxystore.serialize import deserialize
from proxystore.serialize import serialize

data: dict[str, bytes] = {}


class Lib:
    """Mock ucp Lib implementation."""

    def __init__(self):
        pass

    class exceptions:  # noqa: N801
        """Mock Lib exceptions implementation."""

        class UCXNotConnected(Exception):  # noqa: 818
            """Mock Exception implementation."""

            pass


_libs = Lib()


class MockEndpoint:
    """Mock Endpoint.

    This class mocks all the expected behavior of the UCXServer
    on the client side.
    """

    def __init__(self, server: bool = False):
        self.last_rpc: RPC | None = None
        self.server = server
        self.is_closed = False
        self.ping = False

    async def send_obj(self, payload: bytes) -> None:
        """Mock the `ucp.send_obj` function.

        Args:
            payload: The serialized object communicated.
        """
        if payload == b'ping':
            self.ping = True
            return

        rpc = deserialize(payload)

        if rpc.operation == 'evict':
            data.pop(rpc.key.obj_id, None)
        elif rpc.operation == 'put':
            data[rpc.key.obj_id] = rpc.data

        self.last_rpc = rpc

    async def recv_obj(self) -> bytes:
        """Mock the `ucp.recv_obj` function."""
        if self.ping:
            self.ping = False
            return b'pong'

        if self.last_rpc is None:
            raise AssertionError('Called recv_obj before send_obj.')

        response = RPCResponse(
            operation=self.last_rpc.operation,
            key=self.last_rpc.key,
        )
        if response.operation == 'exists':
            response.exists = response.key.obj_id in data
        elif response.operation == 'get':
            response.data = data.get(response.key.obj_id, None)

        return serialize(response)

    async def close(self) -> None:
        """Mock close implementation."""
        self.is_closed = True

    def closed(self) -> bool:
        """Mock closed implementation."""
        return self.is_closed


class Listener:  # pragma: no cover
    """Mock listener implementation."""

    called: bool

    def __init__(self) -> None:
        self.called = False

    def close(self) -> None:
        """Close implementation."""
        pass

    def closed(self) -> bool:
        """Mock closed."""
        if not self.called:
            self.called = True
            return False
        return True


def create_listener(handler: Any, port: int) -> Any:  # pragma: no cover
    """Create_listener mock implementation.

    Args:
        handler: The communication handler.
        port: The communication port.
    """
    return Listener()


async def create_endpoint(host: str, port: int) -> MockEndpoint:
    """Create endpoint mock implementation."""
    return MockEndpoint()


def get_address(ifname: str | None = None) -> str:
    """Mock get_address implementation."""
    return '127.0.0.1'
