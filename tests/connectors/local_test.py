from __future__ import annotations

from typing import Any

from proxystore.connectors.local import LocalConnector
from proxystore.connectors.local import LocalKey
from proxystore.serialize import BytesLike


def test_connector_dict() -> None:
    d: dict[LocalKey, BytesLike] = {}
    connector1 = LocalConnector(d)
    key = connector1.put(b'value')

    connector2 = LocalConnector(d)
    assert connector2.get(key) == b'value'

    connector3 = LocalConnector()
    assert connector3.get(key) is None


def test_close_never_clears() -> None:
    connector = LocalConnector()
    key = connector.put(b'value')
    assert connector.close(clear=True) is False
    assert connector.get(key) == b'value'


class _RacingDict(dict[LocalKey, BytesLike]):
    # Acts like another thread evicting the key right after it is checked.
    def __contains__(self, key: Any) -> bool:
        found = super().__contains__(key)
        super().pop(key, None)
        return found


def test_evict_key_removed_by_another_thread() -> None:
    connector = LocalConnector(_RacingDict())
    key = connector.put(b'value')
    connector.evict(key)
    assert not connector.exists(key)
