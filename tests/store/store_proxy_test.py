from __future__ import annotations

from typing import Any
from typing import Generator

import pytest

import proxystore
from proxystore.connectors.local import LocalConnector
from proxystore.proxy import Proxy
from proxystore.proxy import ProxyLocker
from proxystore.serialize import deserialize
from proxystore.serialize import serialize
from proxystore.store import get_store
from proxystore.store import register_store
from proxystore.store import unregister_store
from proxystore.store.base import Store
from proxystore.store.exceptions import NonProxiableTypeError
from proxystore.store.exceptions import ProxyResolveMissingKeyError
from proxystore.store.factory import StoreFactory
from proxystore.store.utils import get_key


@pytest.fixture(autouse=True)
def _verify_no_registered_stores() -> Generator[None, None, None]:
    yield

    if len(proxystore.store._stores) > 0:  # pragma: no cover
        raise RuntimeError(
            'Test left at least one store registered: '
            f'{tuple(proxystore.store._stores.keys())}.',
        )


def test_factory_resolve(store: Store[LocalConnector]) -> None:
    key = store.put([1, 2, 3])
    f: StoreFactory[Any, list[int]] = StoreFactory(
        key,
        store_config=store.config(),
    )
    assert f() == [1, 2, 3]


def test_factory_evicts_on_resolve(store: Store[LocalConnector]) -> None:
    key = store.put([1, 2, 3])
    f: StoreFactory[Any, list[int]] = StoreFactory(
        key,
        store_config=store.config(),
        evict=True,
    )
    assert store.exists(key)
    assert f() == [1, 2, 3]
    assert not store.exists(key)


def test_factory_recreates_store() -> None:
    with Store('test', LocalConnector()) as store:
        key = store.put([1, 2, 3])
        f: StoreFactory[Any, list[int]] = StoreFactory(
            key,
            store_config=store.config(),
        )
        assert get_store(store.name) is None
        assert f() == [1, 2, 3]
        new_store = get_store(store.name)
        assert new_store is not None
        assert store.config() == new_store.config()

        unregister_store(store)


def test_factory_resolve_async(store: Store[LocalConnector]) -> None:
    key = store.put([1, 2, 3])
    f: StoreFactory[Any, list[int]] = StoreFactory(
        key,
        store_config=store.config(),
    )
    f.resolve_async()
    assert f._obj_future is not None
    assert f() == [1, 2, 3]
    assert f._obj_future is None


def test_factory_is_serializable(store: Store[LocalConnector]) -> None:
    key = store.put([1, 2, 3])
    f: StoreFactory[Any, list[int]] = StoreFactory(
        key,
        store_config=store.config(),
    )
    f_bytes = serialize(f)
    f = deserialize(f_bytes)
    assert f() == [1, 2, 3]


def test_factory_serialization_async(store: Store[LocalConnector]) -> None:
    key = store.put([1, 2, 3])
    f1: StoreFactory[Any, list[int]] = StoreFactory(
        key,
        store_config=store.config(),
    )
    # Want to make sure the future created here does not cause serialization
    # to fail
    f1.resolve_async()
    f2_bytes = serialize(f1)
    f2 = deserialize(f2_bytes)
    assert f1() == f2() == [1, 2, 3]


def test_proxy(store: Store[LocalConnector]) -> None:
    p = store.proxy([1, 2, 3])
    assert isinstance(p, Proxy)
    assert p == [1, 2, 3]
    key = get_key(p)
    assert key is not None
    assert store.get(key) == [1, 2, 3]


def test_proxy_from_key(store: Store[LocalConnector]) -> None:
    key = store.put([1, 2, 3])
    p: Proxy[list[int]] = store.proxy_from_key(key)
    assert isinstance(p, Proxy)
    assert p == [1, 2, 3]


def test_proxy_missing_key(store: Store[LocalConnector]) -> None:
    proxy = store.proxy([1, 2, 3])
    key = get_key(proxy)
    store.evict(key)
    assert not store.exists(key)

    with pytest.raises(ProxyResolveMissingKeyError):
        proxy.__factory__.resolve()

    proxy = store.proxy_from_key(key=key)
    with pytest.raises(ProxyResolveMissingKeyError):
        proxy()


def test_proxy_bad_serializer(store: Store[LocalConnector]) -> None:
    def _serialize(s: str) -> str:
        return s

    with pytest.raises(TypeError):
        # String will not be serialized and should raise error when putting
        # array into Redis
        store.proxy('mystring', serializer=_serialize)  # type: ignore[arg-type]


def test_proxy_resolve_none_type(store: Store[LocalConnector]) -> None:
    # https://github.com/proxystore/proxystore/issues/311
    # We have to put None in the store first then create a proxy from the
    # key because store.proxy(None) will just return None as a shortcut
    # because it is a singleton type.
    key = store.put(None)
    p: Proxy[None] = store.proxy_from_key(key)
    assert isinstance(p, Proxy)
    assert isinstance(p, type(None))


def test_proxy_recreates_store() -> None:
    with Store('test', LocalConnector(), cache_size=0) as store:
        register_store(store)

        p: Proxy[list[int]] = store.proxy([1, 2, 3])
        key = get_key(p)
        assert key is not None

        # Unregister store so proxy recreates it when resolved
        unregister_store(store)

        # Resolve the proxy
        assert p == [1, 2, 3]

        # The store that created the proxy had cache_size=0 so the restored
        # store should also have cache_size=0.
        s = get_store(store.name)
        assert store.cache.maxsize == 0
        assert s is not None
        assert not s.is_cached(key)

        unregister_store(store)


def test_proxy_skip_nonproxiable(store: Store[LocalConnector]) -> None:
    for t in (None, True, False):
        p = store.proxy(t, skip_nonproxiable=True)
        assert not isinstance(p, Proxy)
        assert p is t


def test_proxy_nonproxiable_error(store: Store[LocalConnector]) -> None:
    for t in (None, True, False):
        with pytest.raises(NonProxiableTypeError):
            store.proxy(t, skip_nonproxiable=False)


def test_proxy_batch(store: Store[LocalConnector]) -> None:
    values = ['test_value1', 'test_value2', 'test_value3']
    proxies: list[Proxy[str]] = store.proxy_batch(values)
    for p, v in zip(proxies, values):
        assert p == v


def test_proxy_batch_custom_serializer(store: Store[LocalConnector]) -> None:
    values = [b'test_value1', b'test_value2', b'test_value3']
    proxies: list[Proxy[bytes]] = store.proxy_batch(
        values,
        serializer=lambda s: s,
        deserializer=lambda s: s,
    )
    for p, v in zip(proxies, values):
        assert p == v


def test_proxy_batch_skip_nonproxiable(store: Store[LocalConnector]) -> None:
    v1 = store.proxy_batch([None, True, False], skip_nonproxiable=True)
    assert all(not isinstance(v, Proxy) for v in v1)

    # Test mixed proxies and constants
    inputs = [None, 'string', False, True, [1, 2, 3], 'string']
    should_proxy = [False, True, False, False, True, True]
    v2 = store.proxy_batch(inputs, skip_nonproxiable=True)
    assert all(isinstance(v, Proxy) == e for v, e in zip(v2, should_proxy))


def test_proxy_batch_nonproxiable_error(store: Store[LocalConnector]) -> None:
    with pytest.raises(NonProxiableTypeError):
        # Only one bad value needed to fail
        store.proxy_batch(['string', None, 'string'], skip_nonproxiable=False)


def test_locked_proxy(store: Store[LocalConnector]) -> None:
    assert isinstance(store.locked_proxy([1, 2, 3]), ProxyLocker)


def test_locked_proxy_skip_nonproxiable(store: Store[LocalConnector]) -> None:
    p = store.locked_proxy(None, skip_nonproxiable=True)
    assert not isinstance(p, Proxy)
    assert p is None


def test_locked_proxy_nonproxiable_error(store: Store[LocalConnector]) -> None:
    with pytest.raises(NonProxiableTypeError):
        store.locked_proxy(None, skip_nonproxiable=False)
