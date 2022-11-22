"""Store Imports and Initialization Unit Tests."""
from __future__ import annotations

import pytest

from proxystore.factory import SimpleFactory
from proxystore.proxy import Proxy
from proxystore.store import get_store
from proxystore.store import register_store
from proxystore.store import unregister_store
from proxystore.store.exceptions import ProxyStoreFactoryError
from proxystore.store.exceptions import StoreExistsError
from proxystore.store.local import LocalStore
from proxystore.store.redis import RedisStore


def test_store_registration() -> None:
    """Test registering and unregistering stores directly."""
    store = LocalStore(name='test')

    register_store(store)
    assert get_store('test') == store

    with pytest.raises(StoreExistsError):
        register_store(store)
    register_store(store, exist_ok=True)

    unregister_store(store.name)
    assert get_store('test') is None

    # does not raise error
    unregister_store('not a valid store name')


def test_lookup_by_proxy(local_store, redis_store) -> None:
    """Make sure get_store works with a proxy."""
    # Init by enum
    local = LocalStore('local', **local_store.kwargs)
    redis = RedisStore('redis', **redis_store.kwargs)
    register_store(local)
    register_store(redis)

    # Make a proxy with both
    local_proxy: Proxy[list[int]] = local.proxy([1, 2, 3])
    redis_proxy: Proxy[list[int]] = redis.proxy([1, 2, 3])

    # Make sure both look up correctly
    sr = get_store(redis_proxy)
    assert sr is not None
    assert sr.name == redis.name
    sl = get_store(local_proxy)
    assert sl is not None
    assert sl.name == local.name

    # Make a proxy without an associated store
    f = SimpleFactory([1, 2, 3])
    p = Proxy(f)
    with pytest.raises(ProxyStoreFactoryError):
        get_store(p)

    unregister_store('local')
    unregister_store('redis')
