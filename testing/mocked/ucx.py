"""UCX mocker implementation."""
from __future__ import annotations

from typing import Any

from proxystore.serialize import deserialize
from proxystore.serialize import SerializationError
from proxystore.serialize import serialize

data = {}


class Lib:
    """Mock ucp Lib implementation."""

    def __init__(self):
        """Mock lib init implementation."""
        pass

    class exceptions:  # noqa: N801
        """Mock Lib exceptions implementation."""

        class UCXNotConnected(Exception):  # noqa: 818
            """Mock Exception implementation."""

            pass


_libs = Lib()


class MockEndpoint:
    """Mock Endpoint."""

    last_event: str
    key: str
    response: str | int
    req: Any
    server: Any
    is_closed: bool

    def __init__(self, server=False):
        """Initialize a MockEndpoint."""
        self.key = ''
        self.last_event = ''
        self.response = ''
        self.req = None
        self.server = server
        self.is_closed = False

    async def send_obj(self, req: Any) -> None:
        """Mock the `ucp.send_obj` function.

        Args:
            req (Any): the object to communicate

        """
        self.req = None
        if self.server:
            self.req = req
            return self.req

        try:
            event = deserialize(req)
        except SerializationError:
            event = {}
            event['op'] = 'exists'
            event['key'] = ''
            self.response = 1

        if event['op'] == 'set':
            data[event['key']] = event['data']

        self.key = event['key']
        self.last_event = event['op']

    async def recv_obj(self) -> Any:
        """Mock the `ucp.recv_obj` function."""
        from proxystore.store.dim.utils import Status

        if self.req is not None:
            return self.req

        if self.last_event == 'get':
            try:
                return data[self.key]
            except KeyError as e:
                return serialize(Status(success=False, error=e))
        elif self.last_event == 'exists':
            if self.key != '':
                return serialize(self.key in data)
            else:
                return self.response
        elif self.last_event == 'evict':
            data.pop(self.key, None)
            return serialize(Status(success=True, error=None))
        return serialize(True)

    async def close(self) -> None:
        """Mock close implementation."""
        self.is_closed = True

    def closed(self) -> bool:
        """Mock closed implementation."""
        return self.is_closed


class Listener:
    """Mock listener implementation."""

    called: bool

    def __init__(self) -> None:
        """Mock listener init implementation."""
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


def create_listener(handler: Any, port: int) -> Any:
    """Create_listener mock implementation.

    Args:
        handler (Any): the communication handler
        port (int): the communication port

    """
    return Listener()


async def create_endpoint(
    host: str,
    port: int,
) -> MockEndpoint:
    """Create endpoint mock implementation."""
    return MockEndpoint()
