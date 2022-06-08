"""Store Imports and Initialization Unit Tests."""
from __future__ import annotations

import pytest

import proxystore as ps
from proxystore.factory import SimpleFactory
from proxystore.proxy import Proxy
from proxystore.store import ProxyStoreFactoryError
from proxystore.store import StoreExistsError
from proxystore.store import STORES
from proxystore.store import UnknownStoreError
from proxystore.store.local import LocalStore


def test_imports() -> None:
    """Test imports."""
    import proxystore.store as store

    assert callable(store.get_store)
    assert callable(store.init_store)

    from proxystore.store import get_store, init_store

    assert callable(get_store)
    assert callable(init_store)

    from proxystore.store.local import LocalStore

    LocalStore(name='local')
    ps.store.local.LocalStore(name='local')

    assert callable(ps.store.init_store)


def test_init_store(local_store, redis_store) -> None:
    """Test init_store/get_store."""
    # Init by str name
    local = ps.store.init_store(
        local_store.type,
        name='local',
        **local_store.kwargs,
    )
    assert isinstance(local, ps.store.local.LocalStore)
    redis = ps.store.init_store(
        'redis',
        name='redis',
        **redis_store.kwargs,
    )
    assert isinstance(redis, ps.store.redis.RedisStore)

    assert local == ps.store.get_store('local')
    assert redis == ps.store.get_store('redis')

    ps.store._stores = {}

    # Init by enum
    local = ps.store.init_store(
        STORES.LOCAL,
        name='local',
        **local_store.kwargs,
    )
    assert isinstance(local, ps.store.local.LocalStore)
    redis = ps.store.init_store(
        STORES.REDIS,
        name='redis',
        **redis_store.kwargs,
    )
    assert isinstance(redis, ps.store.redis.RedisStore)

    assert local == ps.store.get_store('local')
    assert redis == ps.store.get_store('redis')

    # Init by class type
    local = ps.store.init_store(
        ps.store.local.LocalStore,
        name='local',
        **local_store.kwargs,
    )
    assert isinstance(local, ps.store.local.LocalStore)

    ps.store._stores = {}

    # Specify name to have multiple stores of same type
    local1 = ps.store.init_store(STORES.LOCAL, 'l1', **local_store.kwargs)
    ps.store.init_store(STORES.LOCAL, 'l2', **local_store.kwargs)

    assert ps.store.get_store('l1') is not ps.store.get_store('l2')

    # Should overwrite old store
    ps.store.init_store(STORES.LOCAL, 'l1', **local_store.kwargs)
    assert local1 is not ps.store.get_store('l1')

    # Return None if store with name does not exist
    assert ps.store.get_store('unknown') is None


def test_get_enum_by_type() -> None:
    """Test getting enum with type."""
    t = STORES.get_str_by_type(ps.store.local.LocalStore)
    assert isinstance(t, str)
    assert STORES[t].value == ps.store.local.LocalStore

    class FakeStore(ps.store.base.Store):
        """FakeStore type."""

        pass

    with pytest.raises(KeyError):
        STORES.get_str_by_type(FakeStore)


def test_init_store_raises() -> None:
    """Test init_store raises."""
    with pytest.raises(UnknownStoreError):
        # Raise error because name cannot be found in STORES
        ps.store.init_store('unknown', name='')

    with pytest.raises(UnknownStoreError):
        # Raises error because type is not a subclass of Store
        class TestStore:
            pass

        ps.store.init_store(TestStore, name='')


def test_register_unregister_store() -> None:
    """Test registering and unregistering stores directly."""
    store = LocalStore(name='test')

    ps.store.register_store(store)
    assert ps.store.get_store('test') == store

    with pytest.raises(StoreExistsError):
        ps.store.register_store(store)
    ps.store.register_store(store, exist_ok=True)

    ps.store.unregister_store(store.name)
    assert ps.store.get_store('test') is None

    # does not raise error
    ps.store.unregister_store('not a valid store name')


def test_lookup_by_proxy(local_store, redis_store) -> None:
    """Make sure get_store works with a proxy."""
    # Init by enum
    local = ps.store.init_store(
        STORES.LOCAL,
        name='local',
        **local_store.kwargs,
    )
    redis = ps.store.init_store(
        STORES.REDIS,
        name='redis',
        **redis_store.kwargs,
    )

    # Make a proxy with both
    local_proxy = local.proxy([1, 2, 3])
    redis_proxy = redis.proxy([1, 2, 3])

    # Make sure both look up correctly
    assert ps.store.get_store(redis_proxy).name == redis.name
    assert ps.store.get_store(local_proxy).name == local.name

    # Make a proxy without an associated store
    f = SimpleFactory([1, 2, 3])
    p = Proxy(f)
    with pytest.raises(ProxyStoreFactoryError):
        ps.store.get_store(p)
