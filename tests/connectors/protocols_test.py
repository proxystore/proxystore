from __future__ import annotations

from proxystore.connectors.local import LocalConnector
from proxystore.connectors.local import LocalKey
from proxystore.connectors.protocols import Connector
from proxystore.connectors.protocols import DeferrableConnector


def test_extend_connector_as_deferrable() -> None:
    class _DeferrableLocalConnector(LocalConnector):
        def new_key(self, obj: bytes | None = None) -> LocalKey:
            raise NotImplementedError

        def set(self, key: LocalKey, obj: bytes) -> None:
            raise NotImplementedError

    assert isinstance(LocalConnector(), Connector)
    assert not isinstance(LocalConnector(), DeferrableConnector)

    assert isinstance(_DeferrableLocalConnector(), Connector)
    assert isinstance(_DeferrableLocalConnector(), DeferrableConnector)
