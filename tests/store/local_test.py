"""LocalStore Unit Tests."""
from __future__ import annotations

from proxystore.store.local import LocalStore
from proxystore.store.local import LocalStoreKey


def test_store_dict() -> None:
    """Test LocalStore reusable dict."""
    d: dict[LocalStoreKey, bytes] = {}
    store1 = LocalStore('local1', store_dict=d)
    key = store1.set(123)

    store2 = LocalStore('local2', store_dict=d)
    assert store2.get(key) == 123

    store3 = LocalStore('local3')
    assert store3.get(key) is None
