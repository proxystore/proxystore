"""UCX mocker implementation."""
from __future__ import annotations

from pickle import loads
from typing import Any


data = {}


class MockEndpoint:
    """Mock Endpoint."""

    last_event: str
    key: str
    response: str

    def __init__(self):
        """Initializes the MockEndpoint."""
        self.key = ''
        self.last_event = ''
        self.response = ''

    async def send_obj(self, obj: Any) -> None:
        """Mocks the `ucp.send_obj` function.

        Args:
            obj (Any): the object to communicate

        """
        event = loads(obj)

        if event['op'] == 'set':
            data[event['key']] = event['data']

        self.key = event['key']
        self.last_event = event['op']

    async def recv_obj(self) -> Any:
        """Mocks the `ucp.recv_obj` function."""
        if self.last_event == 'get':
            try:
                return data[self.key]
            except KeyError:
                return None
        elif self.last_event == 'exists':
            return self.key in data
        elif self.last_event == 'evict':
            try:
                del data[self.key]
            except KeyError:
                pass
            return None
        return True

    async def close(self) -> None:
        """Mock close implementation."""
        return None

    def closed(self) -> bool:
        """Mock closed implementation."""
        return True


class Listener:
    """Mock listener implementation."""

    def __init__(self) -> None:
        """Mock listener init implementation."""
        pass

    def closed(self) -> bool:
        """Mock closed."""
        return True


def get_address(ifname: str) -> str:
    """Get address mock implementation."""
    return ifname


def create_listener(handler: Any, port: int) -> Any:
    """Create_listener mock implementation.

    Args:
        handler (Any): the communication handler
        port (int): the communication port

    """
    return Listener()


async def create_endpoint(host: str, port: int) -> MockEndpoint:
    """Create endpoint mock implementation."""
    return MockEndpoint()
