"""Store Stat Tracking Tests."""
from __future__ import annotations

from typing import Any
from typing import NamedTuple

import pytest

import proxystore.store
from proxystore.proxy import Proxy
from proxystore.proxy import resolve
from proxystore.store import get_store
from proxystore.store import init_store
from proxystore.store.base import Store
from proxystore.store.base import StoreFactory
from proxystore.store.utils import get_key
from testing.store_utils import FIXTURE_LIST


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_init_stats(store_fixture, request) -> None:
    """Test Initializing Stat tracking."""
    store_config = request.getfixturevalue(store_fixture)

    with store_config.type(store_config.name, **store_config.kwargs) as store:
        with pytest.raises(ValueError):
            # Check raises an error because stats are not tracked by default
            store.stats('key')

        store = store_config.type(
            store_config.name,
            **store_config.kwargs,
            stats=True,
        )

        assert isinstance(store.stats('key'), dict)


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_stat_tracking(store_fixture, request) -> None:
    """Test stat tracking of store."""
    store_config = request.getfixturevalue(store_fixture)

    store = init_store(
        store_config.type,
        store_config.name,
        **store_config.kwargs,
        stats=True,
    )

    p: Proxy[list[int]] = store.proxy([1, 2, 3])
    key = get_key(p)
    assert key is not None
    resolve(p)

    stats = store.stats(key)

    assert 'get' in stats
    assert 'set' in stats

    assert stats['get'].calls == 1
    assert stats['set'].calls == 1

    # stats should return a copy of the stats, not the internal data
    # structures so calling get again should not effect anything.
    store.get(key)

    assert stats['get'].calls == 1

    stats = store.stats('missing_key')

    assert len(stats) == 0

    store.close()


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_get_stats_with_proxy(store_fixture, request) -> None:
    """Test Get Stats with Proxy."""
    store_config = request.getfixturevalue(store_fixture)

    store = init_store(
        store_config.type,
        store_config.name,
        **store_config.kwargs,
        stats=True,
    )

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


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_factory_preserves_tracking(store_fixture, request) -> None:
    """Test Factories Preserve the Stat Tracking Flag."""
    store_config = request.getfixturevalue(store_fixture)

    store: Store[Any] | None
    store = init_store(
        store_config.type,
        store_config.name,
        **store_config.kwargs,
        stats=True,
    )

    p: Proxy[list[int]] = store.proxy([1, 2, 3])
    key = get_key(p)
    assert key is not None

    # Force delete store so proxy recreates it when resolved
    proxystore.store._stores = {}

    # Resolve the proxy
    assert p == [1, 2, 3]
    store = get_store(store_config.name)
    assert store is not None

    assert isinstance(store.stats(key), dict)
    assert store.stats(key)['get'].calls == 1

    store.close()
