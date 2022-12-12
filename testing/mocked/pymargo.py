"""PyMargo mocker implementation."""
from __future__ import annotations

from typing import Any

from proxystore.serialize import serialize

# pymargo vars
client = 'client'
server = 'server'

# server dictionary
data_dict = {}


class MargoException(Exception):
    """Mock Exception implementation."""

    def __init__(self):  # pragma: no cover
        """Exception init implementation."""
        pass


class Address:  # pragma: no cover
    """Mock Address implementation."""

    def __init__(self) -> None:
        """Mock Address initialization."""
        pass

    def shutdown(self) -> None:
        """Mock shutdown."""
        pass


class Engine:
    """Mock Engine implementation."""

    def __init__(
        self,
        url: str,
        mode: str = server,
        use_progress_thread: bool = False,
    ) -> None:
        """Mock Engine initialization."""
        self.url = url

    def addr(self) -> str:  # pragma: no cover
        """Get Mock Engine address."""
        return self.url

    def on_finalize(self, func: Any) -> None:  # pragma: no cover
        """Mock engine on_finalize."""
        pass

    def enable_remote_shutdown(self) -> None:  # pragma: no cover
        """Mock engine enable_remote_shutdown."""
        pass

    def wait_for_finalize(self) -> None:  # pragma: no cover
        """Mock engine wait_for_finalize."""
        pass

    def create_bulk(self, data: bytes, bulk_type: str) -> Bulk:
        """Mock create_bulk implementation."""
        return Bulk(data)

    def lookup(self, addr: str) -> Engine:  # pragma: no cover
        """Mock lookup implementation."""
        return self

    def shutdown(self) -> None:  # pragma: no cover
        """Mock shutdown."""
        pass

    def finalize(self) -> None:  # pragma: no cover
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
    ) -> None:  # pragma: no cover
        """Mock transfer."""
        if bulk_size == -1:  # pragma: no cover
            raise ValueError

        if bulk_op == 'pull':  # pragma: no cover
            try:
                assert isinstance(local_bulk.data, bytearray)
                local_bulk.data[:] = bulk_str.data
            except AssertionError:
                local_bulk.data = bulk_str.data

        else:
            try:
                assert isinstance(bulk_str.data, bytearray)
                bulk_str.data[:] = local_bulk.data
            except AssertionError:
                bulk_str.data = local_bulk.data

    def register(self, funcname: str, *args: Any) -> RPC:  # pragma: no cover
        """Mock register.

        Args:
            funcname (str): the function name

        """
        return RPC(funcname)


class RPC:
    """Mock RPC implementation."""

    def __init__(self, name: str) -> None:  # pragma: no cover
        """Mock RPC initialization."""
        self.name = name

    def on(self, addr: str) -> Any:  # pragma: no cover
        """Mock RPC on implementation."""
        return self.mockfunc

    def mockfunc(
        self,
        array_str: Bulk,
        size: int,
        key: str,
    ) -> Any:  # pragma: no cover
        """Mockfunc implementation."""
        from proxystore.store.dim.utils import Status

        if self.name == 'set':
            data_dict[key] = array_str.data
            return serialize(Status(True, None))
        elif self.name == 'get':
            if key not in data_dict:
                return serialize(
                    Status(
                        False,
                        Exception('MockException occurred in `get_bytes`'),
                    ),
                )
            else:
                try:
                    assert isinstance(array_str.data, bytearray)
                    array_str.data[:] = data_dict[key]
                except AssertionError:  # pragma: no cover
                    array_str.data = data_dict[key]
            return serialize(Status(True, None))
        elif self.name == 'evict':
            if key not in data_dict:
                return serialize(
                    Status(
                        False,
                        Exception('MockException occurred in `evict`'),
                    ),
                )
            else:
                del data_dict[key]
            return serialize(Status(True, None))
        else:
            try:
                assert isinstance(array_str.data, bytearray)
                array_str.data[:] = serialize(key in data_dict)
            except AssertionError:  # pragma: no cover
                array_str.data = serialize(key in data_dict)
            return serialize(Status(True, None))


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

    data: bytearray | bytes

    def __init__(self, data: bytearray | bytes) -> None:
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
