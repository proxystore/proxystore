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

    def create_bulk(self, data: bytes, bulk_type: str) -> Bulk:
        """Mock create_bulk implementation."""
        return Bulk(data)

    def lookup(self, addr: str) -> Engine:
        """Mock lookup implementation."""
        return self

    def shutdown(self) -> None:
        """Mock shutdown."""
        pass

    def finalize(self) -> None:
        """Mock finalize."""
        pass

    def transfer(
        self,
        bulk_op: str,
        addr: str,
        bulk_str: Bulk,
        oo: int,
        local_bulk: Bulk,
        lo: int,
        bulk_size: int,
    ) -> None:
        """Mock transfer."""
        if bulk_size == -1 or len(bulk_str.data) == 2:
            raise Exception

        if bulk_op == 'pull':
            local_bulk.data[:] = bulk_str.data
        else:
            bulk_str.data[:] = local_bulk.data

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

    def mockfunc(
        self,
        array_str: Bulk,
        size: int,
        key: str,
    ) -> Any:
        """Mockfunc implementation."""
        from proxystore.store.dim.utils import Status

        if self.name == 'set_bytes':
            data_dict[key] = array_str.data
            return Status(True, None)
        elif self.name == 'get_bytes':
            if key not in data_dict:
                return Status(
                    False,
                    Exception('MockException occurred in `get_bytes`'),
                )
            else:
                array_str.data[:] = data_dict[key]
            return Status(True, None)
        elif self.name == 'evict':
            if key not in data_dict:
                return Status(
                    False,
                    Exception('MockException occurred in `evict`'),
                )
            else:
                del data_dict[key]
            return Status(True, None)
        else:
            array_str.data[:] = bytes(str(int(key in data_dict)), 'utf-8')
            return Status(True, None)


class MockBulkMod:
    """MockBulkMod implementation."""

    # bulk variable
    read_write = 'rw'
    read_only = 'r'
    write_only = 'w'
    push = 'push'
    pull = 'pull'


bulk = MockBulkMod()


class Bulk:
    """Mock Bulk implementation."""

    data: bytearray

    def __init__(self, data) -> None:
        """Mock Bulk initialization."""
        self.data = data


class Handle:
    """Mock Handle implementation."""

    response: Any

    def __init__(self) -> None:
        """Mock handle initialization."""
        from proxystore.store.dim.utils import Status

        self.response = Status(True, None)

    def respond(
        self,
        status: Any,
    ) -> Any:
        """Mock respond."""
        self.response = status
        return self.response

    def get_addr(self) -> str:
        """Mock addr."""
        return 'addr'


class RemoteFunction:
    """Remote function implementation."""

    pass
