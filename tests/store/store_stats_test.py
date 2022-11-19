"""Store Stat Tracking Tests."""
from __future__ import annotations

from typing import Any
from typing import NamedTuple

import pytest

from proxystore.proxy import Proxy
from proxystore.proxy import resolve
from proxystore.store import get_store
from proxystore.store import register_store
from proxystore.store import unregister_store
from proxystore.store.base import Store
from proxystore.store.base import StoreFactory
from proxystore.store.utils import get_key
from testing.stores import StoreFixtureType


def test_init_stats(store_implementation: StoreFixtureType) -> None:
    """Test Initializing Stat tracking."""
    store, store_info = store_implementation

    with pytest.raises(ValueError):
        # Check raises an error because stats are not tracked by default
        store.stats('key')

    store = store_info.type(
        store_info.name,
        **store_info.kwargs,
        stats=True,
    )

    assert isinstance(store.stats('key'), dict)


def test_stat_tracking(store_implementation: StoreFixtureType) -> None:
    """Test stat tracking of store."""
    _, store_info = store_implementation

    store = store_info.type(
        store_info.name,
        **store_info.kwargs,
        stats=True,
    )
    register_store(store)

    p: Proxy[list[int]] = store.proxy([1, 2, 3])
    key = get_key(p)
    assert key is not None
    resolve(p)

    stats = store.stats(key)

    assert 'get' in stats
    assert 'set' in stats
    assert 'get_bytes' in stats
    assert 'set_bytes' in stats

    assert stats['get'].calls == 1
    assert stats['set'].calls == 1
    size = stats['get_bytes'].size_bytes
    assert size is not None and size > 0
    size = stats['set_bytes'].size_bytes
    assert size is not None and size > 0

    # stats should return a copy of the stats, not the internal data
    # structures so calling get again should not effect anything.
    store.get(key)

    assert stats['get'].calls == 1

    stats = store.stats('missing_key')

    assert len(stats) == 0

    unregister_store(store_info.name)


def test_get_stats_with_proxy(store_implementation: StoreFixtureType) -> None:
    """Test Get Stats with Proxy."""
    store, store_info = store_implementation

    store = store_info.type(
        store_info.name,
        **store_info.kwargs,
        stats=True,
    )
    register_store(store)

    p: Proxy[list[int]] = store.proxy([1, 2, 3])

    # Proxy has not been resolved yet so get/resolve should not exist
    stats = store.stats(p)
    assert 'get' not in stats
    assert 'set' in stats
    assert 'resolve' not in stats

    # Resolve proxy and verify get/resolve exist
    resolve(p)
    stats = store.stats(p)
    assert 'get' in stats
    assert 'resolve' in stats

    # Check that resolve stats are unique to that proxy and not merged into
    # the store's stats
    key = get_key(p)
    assert key is not None
    stats = store.stats(key)
    assert 'resolve' not in stats

    # Since we monkeypatch the stats into the factory, we need to handle
    # special cases of Factories without the _stats attr or the _stats attr
    # is None.
    class FactoryMissingStats(StoreFactory[Any, Any]):
        def __init__(self, key: NamedTuple):
            self.key = key

        def resolve(self):
            pass

    p = Proxy(FactoryMissingStats(key))
    resolve(p)
    stats = store.stats(p)
    assert 'resolve' not in stats

    class FactoryNoneStats(StoreFactory[Any, Any]):
        def __init__(self, key: NamedTuple):
            self.key = key
            self.stats = None

        def resolve(self):
            pass

    p = Proxy(FactoryNoneStats(key))
    resolve(p)
    stats = store.stats(p)
    assert 'resolve' not in stats

    unregister_store(store_info.name)


def test_factory_preserves_tracking(
    store_implementation: StoreFixtureType,
) -> None:
    """Test Factories Preserve the Stat Tracking Flag."""
    _, store_info = store_implementation

    store: Store[Any] | None
    store = store_info.type(
        store_info.name,
        **store_info.kwargs,
        stats=True,
    )
    register_store(store)

    p: Proxy[list[int]] = store.proxy([1, 2, 3])
    key = get_key(p)
    assert key is not None

    # Remove store so proxy recreates it when resolved
    unregister_store(store_info.name)

    # Resolve the proxy
    assert p == [1, 2, 3]
    store = get_store(store_info.name)
    assert store is not None

    assert isinstance(store.stats(key), dict)
    assert store.stats(key)['get'].calls == 1

    unregister_store(store_info.name)
