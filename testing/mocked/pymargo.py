"""PyMargo mocker implementation."""
from __future__ import annotations

from typing import Any

from proxystore.connectors.dim.models import DIMKey
from proxystore.connectors.dim.models import RPCResponse
from proxystore.serialize import serialize

# pymargo vars
client = 'client'
server = 'server'

# server dictionary
data_dict: dict[str, bytes] = {}


class MargoException(Exception):  # pragma: no cover  # noqa: N818
    """Mock Exception implementation."""

    def __init__(self):
        pass


class Address:  # pragma: no cover
    """Mock Address implementation."""

    def __init__(self, url):
        self.addr = url

    def shutdown(self) -> None:
        """Mock shutdown."""
        pass

    def __str__(self) -> str:
        return self.addr


class Engine:
    """Mock Engine implementation."""

    def __init__(
        self,
        url: str,
        mode: str = server,
        use_progress_thread: bool = False,
    ) -> None:
        if url is None or '://' not in url:
            self.address = Address('tcp://127.0.0.1:1234')
        else:
            self.address = Address(url)

    def addr(self) -> Address:
        """Mock Engine addr implementation."""
        return self.address

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

    def lookup(self, addr: str) -> Address:
        """Mock lookup implementation."""
        return self.address

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
        if bulk_size == -1:  # pragma: no cover
            raise ValueError

        if bulk_op == 'pull':
            assert isinstance(local_bulk.data, bytearray)
            local_bulk.data[:] = bulk_str.data

        else:
            assert isinstance(bulk_str.data, bytearray)
            bulk_str.data[:] = local_bulk.data

    def register(self, funcname: str, *args: Any) -> RemoteFunction:
        """Mock register.

        Args:
            funcname: The function name.
            args: Additional positional arguments.
        """
        return RemoteFunction(funcname)


class RemoteFunction:
    """Mock RemoteFunction implementation."""

    def __init__(self, name: str) -> None:
        self.name = name

    def on(self, addr: str) -> Any:
        """Mock RemoteFunction on implementation."""
        return self.mockfunc

    def mockfunc(
        self,
        array_str: Bulk,
        size: int,
        key: DIMKey,
    ) -> Any:
        """Mockfunc implementation."""
        if self.name == 'put':
            data_dict[key.obj_id] = array_str.data
            return serialize(RPCResponse(operation='put', key=key))
        elif self.name == 'get':
            if key.obj_id not in data_dict:
                return serialize(
                    RPCResponse(operation='get', key=key, exists=False),
                )
            else:
                assert isinstance(array_str.data, bytearray)
                array_str.data[:] = data_dict[key.obj_id]
            return serialize(
                RPCResponse(operation='get', key=key, exists=True),
            )
        elif self.name == 'evict':
            if key.obj_id not in data_dict:
                return serialize(RPCResponse(operation='evict', key=key))
            else:
                del data_dict[key.obj_id]
            return serialize(RPCResponse(operation='evict', key=key))
        else:
            return serialize(
                RPCResponse(
                    operation='exists',
                    key=key,
                    exists=(key.obj_id in data_dict),
                ),
            )


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

    def __init__(self, data: bytearray | bytes) -> None:
        self.data = data


class Handle:
    """Mock Handle implementation."""

    def __init__(self) -> None:
        self.response: Any = None

    def respond(self, response: Any) -> None:
        """Mock respond."""
        self.response = response
        return None

    def get_addr(self) -> str:
        """Mock addr."""
        return 'addr'
