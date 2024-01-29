from __future__ import annotations

from proxystore.connectors.local import LocalConnector
from proxystore.connectors.local import LocalKey


def test_connector_dict() -> None:
    d: dict[LocalKey, bytes] = {}
    connector1 = LocalConnector(d)
    key = connector1.put(b'value')

    connector2 = LocalConnector(d)
    assert connector2.get(key) == b'value'

    connector3 = LocalConnector()
    assert connector3.get(key) is None
