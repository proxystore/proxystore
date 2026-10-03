from __future__ import annotations

import threading
from typing import Any
from unittest import mock

import pytest

from proxystore.connectors.local import LocalConnector
from proxystore.proxy import Proxy
from proxystore.proxy import ProxyResolveError
from proxystore.proxy import resolve
from proxystore.serialize import BytesLike
from proxystore.serialize import SerializationError
from proxystore.store import Store
from proxystore.store.exceptions import ProxyResolveMissingKeyError
from proxystore.store.future import PollingPolicy
from proxystore.store.future import ProxyFuture
from proxystore.store.lifetimes import ContextLifetime


def test_negative_cache_size() -> None:
    with pytest.raises(ValueError, match='Cache size cannot be negative'):
        Store(LocalConnector(), cache_size=-1)


def test_store_name_and_id() -> None:
    with Store(LocalConnector()) as store:
        assert store.name is None
        assert store.id in repr(store)

    with Store(LocalConnector(), name='test') as store:
        assert store.name == 'test'
        assert 'name=test' in repr(store)


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
    with Store(LocalConnector(), cache_size=0) as store:
        assert store.cache.maxsize == 0
        value = 'test_value'

        # Test cache size 0
        key1 = store.put(value)
        assert store.get(key1) == value
        assert not store.is_cached(key1)

    with Store(LocalConnector(), cache_size=1) as store:
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


def test_custom_deserializer_error(store: Store[LocalConnector]) -> None:
    key = store.put('value')

    def _deserialize(x: BytesLike) -> Any:
        raise Exception

    with pytest.raises(
        SerializationError,
        match='Failed to deserialize object',
    ):
        store.get(key, deserializer=_deserialize)


def test_get_missing_with_sentinel(store: Store[LocalConnector]) -> None:
    missing = object()
    key = store.put(None)
    assert store.get(key, default=missing) is None
    store.evict(key)
    assert store.get(key, default=missing) is missing


def test_get_batch() -> None:
    with Store(LocalConnector(), metrics=True) as store:
        keys = store.put_batch(['value1', None, 'value3'])
        missing_key = store.put('missing')
        store.evict(missing_key)

        # Cache the first object so the batch has hits and misses.
        assert store.get(keys[0]) == 'value1'

        batch_keys = [*keys, missing_key]
        missing = object()
        values = store.get_batch(batch_keys, default=missing)
        assert values == ['value1', None, 'value3', missing]
        assert all(store.is_cached(key) for key in keys)

        assert store.metrics is not None
        metrics = store.metrics.get_metrics(batch_keys)
        assert metrics is not None
        assert metrics.counters['store.get_batch.cache_hits'] == 1
        assert metrics.counters['store.get_batch.cache_misses'] == 3
        assert metrics.times['store.get_batch'].count == 1
        assert metrics.times['store.get_batch.connector'].count == 1

        # All objects are now cached so the connector is not used.
        with mock.patch.object(store.connector, 'get_batch') as mock_get:
            assert store.get_batch(keys) == ['value1', None, 'value3']
            mock_get.assert_not_called()


def test_get_batch_empty(store: Store[LocalConnector]) -> None:
    assert store.get_batch([]) == []


def test_get_batch_custom_deserializer(store: Store[LocalConnector]) -> None:
    keys = store.put_batch([b'a', b'b'], serializer=lambda x: x)
    values = store.get_batch(keys, deserializer=lambda x: bytes(x).upper())
    assert values == [b'A', b'B']


def test_get_batch_deserializer_error(store: Store[LocalConnector]) -> None:
    keys = store.put_batch(['a'])

    def _error(data: BytesLike) -> Any:
        raise ValueError('Oops')

    with pytest.raises(SerializationError, match='Failed to deserialize'):
        store.get_batch(keys, deserializer=_error)


def test_put_batch(store: Store[LocalConnector]) -> None:
    values = ['test_value1', 'test_value2', 'test_value3']

    # Test without keys
    keys = store.put_batch(values)
    for key in keys:
        assert store.exists(key)


def test_put_batch_custom_serializer(store: Store[LocalConnector]) -> None:
    values = ['test_value1', 'test_value2', 'test_value3']

    new_keys = store.put_batch(values, serializer=str.encode)
    for key in new_keys:
        assert store.exists(key)


def test_set() -> None:
    with Store(LocalConnector(), cache_size=1) as store:
        key = store.connector.new_key()
        assert not store.exists(key)
        store._set(key, 'test_value')
        assert store.get(key) == 'test_value'
        assert store.is_cached(key)
        store._set(key, 'new_value')
        assert not store.is_cached(key)


def test_set_bad_connector_type(store: Store[LocalConnector]) -> None:
    key = store.connector.new_key()
    with (
        mock.patch.object(store, 'connector', object()),
        pytest.raises(NotImplementedError, match='DeferrableConnector'),
    ):
        store._set(key, 'new-value')


def test_set_custom_serializer(store: Store[LocalConnector]) -> None:
    key = store.connector.new_key()
    store._set(key, 'test_value', serializer=str.encode)
    assert store.get(key, deserializer=lambda s: s) == b'test_value'

    with pytest.raises(TypeError, match='bytes'):
        store._set(key, 'test_value', serializer=lambda s: s)


def test_future(store: Store[LocalConnector]) -> None:
    future: ProxyFuture[str] = store.future()
    proxy = future.proxy()
    assert not future.done()
    future.set_result('test_value')
    assert future.done()
    assert future.result() == 'test_value'
    assert proxy == 'test_value'


def test_future_result_none(store: Store[LocalConnector]) -> None:
    future: ProxyFuture[None] = store.future()
    future.set_result(None)
    assert future.result(timeout=0) is None


def test_future_result_timeout(store: Store[LocalConnector]) -> None:
    future: ProxyFuture[str] = store.future(
        polling=PollingPolicy(interval=0.001),
    )
    with pytest.raises(TimeoutError):
        future.result(timeout=0.002)


def test_future_result_policy_timeout(store: Store[LocalConnector]) -> None:
    future: ProxyFuture[str] = store.future(
        polling=PollingPolicy(interval=0.001, timeout=0.002),
    )
    with pytest.raises(TimeoutError):
        future.result()
    with pytest.raises(ProxyResolveError) as exc_info:
        resolve(future.proxy())
    assert isinstance(exc_info.value.cause, ProxyResolveMissingKeyError)


@pytest.mark.parametrize('custom_serializer', (True, False))
def test_future_set_exception(
    custom_serializer: bool,
    store: Store[LocalConnector],
) -> None:
    future: ProxyFuture[str] = (
        store.future(
            serializer=str.encode,
            deserializer=lambda b: bytes(b).decode(),
        )
        if custom_serializer
        else store.future()
    )
    proxy = future.proxy()
    assert not future.done()

    future.set_exception(ValueError('Oops'))
    assert future.done()
    with pytest.raises(ValueError, match='Oops'):
        future.result()
    with pytest.raises(ProxyResolveError) as exc_info:
        resolve(proxy)
    assert isinstance(exc_info.value.cause, ValueError)


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
    with (
        mock.patch.object(store, 'connector', object()),
        pytest.raises(NotImplementedError, match='DeferrableConnector'),
    ):
        store.future()


def test_put_lifetime(store: Store[LocalConnector]) -> None:
    with ContextLifetime(store) as lifetime:
        key = store.put('test_value', lifetime=lifetime)

    assert not store.exists(key)


def test_put_batch_lifetime(store: Store[LocalConnector]) -> None:
    values = ['test_value1', 'test_value2', 'test_value3']

    with ContextLifetime(store) as lifetime:
        keys = store.put_batch(values, lifetime=lifetime)

    for key in keys:
        assert not store.exists(key)
