from __future__ import annotations

import threading
from typing import Any
from unittest import mock

import pytest

from proxystore.connectors.local import LocalConnector
from proxystore.proxy import Proxy
from proxystore.store import Store
from proxystore.store.future import ProxyFuture


def test_negative_cache_size() -> None:
    with pytest.raises(ValueError):
        Store('test', LocalConnector(), cache_size=-1)


@pytest.mark.parametrize(
    'value',
    (b'value', 'value', lambda: 'value', ['value1', 'value2', 'value3']),
)
def test_basic_operations(value: Any, store: Store[LocalConnector]) -> None:
    key = store.put(value)

    assert store.exists(key)

    if callable(value):
        c = store.get(key)
        assert c is not None
        assert c() == value()
    else:
        assert store.get(key) == value

    store.evict(key)
    assert not store.exists(key)
    assert not store.is_cached(key)


def test_operations_on_missing_key(store: Store[LocalConnector]) -> None:
    key_fake = store.put(None)
    store.evict(key_fake)

    assert store.get(key_fake) is None
    assert store.get(key_fake, default='alt_value') == 'alt_value'

    assert not store.exists(key_fake)
    store.evict(key_fake)


def test_caching() -> None:
    with Store('test', LocalConnector(), cache_size=0) as store:
        assert store.cache.maxsize == 0
        value = 'test_value'

        # Test cache size 0
        key1 = store.put(value)
        assert store.get(key1) == value
        assert not store.is_cached(key1)

    with Store('test', LocalConnector(), cache_size=1) as store:
        # Add our test value
        key1 = store.put(value)

        # Test caching
        assert not store.is_cached(key1)
        # Cache exists is false but this is still true
        assert store.exists(key1)
        assert store.get(key1) == value
        # Get again comes from cache
        assert store.get(key1) == value
        assert store.is_cached(key1)
        # Cache exists is true shortcut
        assert store.exists(key1)

        # Add second value
        key2 = store.put(value)
        assert store.is_cached(key1)
        assert not store.is_cached(key2)

        # Check cached value flipped since cache size is 1
        assert store.get(key2) == value
        assert not store.is_cached(key1)
        assert store.is_cached(key2)


def test_custom_serializer(store: Store[LocalConnector]) -> None:
    # Pretend serialized string
    s = b'ABC'
    key = store.put(s, serializer=lambda s: s)
    assert store.get(key, deserializer=lambda s: s) == s

    with pytest.raises(TypeError, match='bytes'):
        # Should fail because the array is not already serialized
        store.put([1, 2, 3], serializer=lambda s: s)

    with pytest.raises(TypeError, match='bytes'):
        # Should fail because the array is not already serialized
        store.put_batch([[1, 2, 3]], serializer=lambda s: s)


def test_put_batch(store: Store[LocalConnector]) -> None:
    values = ['test_value1', 'test_value2', 'test_value3']

    # Test without keys
    keys = store.put_batch(values)
    for key in keys:
        assert store.exists(key)


def test_put_batch_custom_serializer(store: Store[LocalConnector]) -> None:
    values = ['test_value1', 'test_value2', 'test_value3']

    new_keys = store.put_batch(values, serializer=lambda s: str.encode(s))
    for key in new_keys:
        assert store.exists(key)


def test_set(store: Store[LocalConnector]) -> None:
    key = store.connector.new_key()
    assert not store.exists(key)
    store._set(key, 'test_value')
    assert store.get(key) == 'test_value'


def test_set_bad_connector_type(store: Store[LocalConnector]) -> None:
    key = store.connector.new_key()
    with mock.patch.object(store, 'connector', object()):
        with pytest.raises(NotImplementedError, match='DeferrableConnector'):
            store._set(key, 'new-value')


def test_set_custom_serializer(store: Store[LocalConnector]) -> None:
    key = store.connector.new_key()
    store._set(key, 'test_value', serializer=lambda s: str.encode(s))
    assert store.get(key, deserializer=lambda s: s) == b'test_value'

    with pytest.raises(TypeError, match='bytes'):
        store._set(key, 'test_value', serializer=lambda s: s)


def test_future(store: Store[LocalConnector]) -> None:
    future: ProxyFuture[str] = store.future()
    proxy = future.proxy()
    future.set_result('test_value')
    assert future.result() == 'test_value'
    assert proxy == 'test_value'


def test_future_in_threads(store: Store[LocalConnector]) -> None:
    future: ProxyFuture[str] = store.future()

    def _foo(
        future: ProxyFuture[str],
        barrier: threading.Barrier,
    ) -> None:
        future.set_result('test_value')
        barrier.wait()

    def _bar(value: Proxy[str], barrier: threading.Barrier) -> None:
        barrier.wait()
        assert value == 'test_value'

    barrier = threading.Barrier(2, timeout=5)
    t_foo = threading.Thread(target=_foo, args=(future, barrier))
    t_bar = threading.Thread(target=_bar, args=(future.proxy(), barrier))

    t_bar.start()
    t_foo.start()

    t_foo.join()
    t_bar.join()


def test_future_bad_connector_type(store: Store[LocalConnector]) -> None:
    with mock.patch.object(store, 'connector', object()):
        with pytest.raises(NotImplementedError, match='DeferrableConnector'):
            store.future()
