"""PyMargo mocker implementation."""
from __future__ import annotations

from typing import Any

# pymargo vars
client = 'client'
server = 'server'

# server dictionary
data_dict = {}


class Engine:
    """Mock Engine implementation."""

    def __init__(self, url: str, mode: str = server) -> None:
        """Mock Engine initialization."""
        self.url = url

    def addr(self) -> str:
        """Get Mock Engine address."""
        return self.url

    def on_finalize(self, func: Any) -> None:
        """Mock engine on_finalize."""
        pass

    def enable_remote_shutdown(self) -> None:
        """Mock engine enable_remote_shutdown."""
        pass

    def wait_for_finalize(self) -> None:
        """Mock engine wait_for_finalize."""
        pass

    def create_bulk(self, data: bytes, bulk_type: str) -> bytes:
        """Mock create_bulk implementation."""
        return data

    def lookup(self, addr: str) -> Engine:
        """Mock lookup implementation."""
        return self

    def shutdown(self) -> None:
        """Mock shutdown."""
        pass

    def finalize(self) -> None:
        """Mock finalize."""
        pass

    def transfer(self, *args: Any) -> None:
        """Mock transfer."""
        pass

    def register(self, funcname: str, *args: Any) -> RPC:
        """Mock register.

        Args:
            funcname (str): the function name

        """
        return RPC(funcname)


class RPC:
    """Mock RPC implementation."""

    def __init__(self, name: str) -> None:
        """Mock RPC initialization."""
        self.name = name

    def on(self, addr: str) -> Any:
        """Mock RPC on implementation."""
        return self.mockfunc

    def mockfunc(self, array_str: bytearray, size: int, key: str) -> str:
        """Mockfunc implementation."""
        if self.name == 'set_bytes':
            data_dict[key] = array_str
            return 'OK'
        elif self.name == 'get_bytes':
            if key not in data_dict:
                return 'ERROR'
            else:
                array_str[:] = data_dict[key]
            return 'OK'
        elif self.name == 'evict':
            if key not in data_dict:
                return 'ERROR'
            else:
                del data_dict[key]
            return 'OK'
        else:
            array_str[:] = bytes(str(int(key in data_dict)), 'utf-8')
            return 'OK'


class MockBulkMod:
    """MockBulkMod implementation."""

    # bulk variable
    read_write = 'rw'
    write_only = 'w'
    push = 'push'
    pull = 'pull'


bulk = MockBulkMod()


class Bulk:
    """Mock Bulk implementation."""

    def __init__(self) -> None:
        """Mock Bulk initialization."""
        pass


class Handle:
    """Mock Handle implementation."""

    def __init__(self) -> None:
        """Mock handle initialization."""
        pass

    def respond(self, text: str) -> str:
        """Mock respond."""
        return text

    def get_address(self) -> str:
        """Mock addr."""
        return 'addr'
