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
    req: Any

    def __init__(self, server=False):
        """Initializes the MockEndpoint."""
        self.key = ''
        self.last_event = ''
        self.response = ''
        self.req = None
        self.server = server

    async def send_obj(self, req: Any) -> None:
        """Mocks the `ucp.send_obj` function.

        Args:
            req (Any): the object to communicate

        """
        self.req = None
        if self.server:
            self.req = req
            return

        event = loads(req)

        if event['op'] == 'set':
            data[event['key']] = event['data']

        self.key = event['key']
        self.last_event = event['op']

    async def recv_obj(self) -> Any:
        """Mocks the `ucp.recv_obj` function."""
        if self.req is not None:
            return self.req

        if self.last_event == 'get':
            try:
                return data[self.key]
            except KeyError:
                return bytes('ERROR', encoding='UTF-8')
        elif self.last_event == 'exists':
            return self.key in data
        elif self.last_event == 'evict':
            try:
                del data[self.key]
            except KeyError:
                return bytes('ERROR', encoding='UTF-8')
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
