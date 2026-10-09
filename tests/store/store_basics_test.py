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


def test_invalid_cache_mode() -> None:
    with pytest.raises(ValueError, match='Cache mode must be'):
        Store(LocalConnector(), cache_mode='other')  # type: ignore[arg-type]


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


def test_custom_serializer() -> None:
    with Store(
        LocalConnector(),
        serializer=str.encode,
        deserializer=lambda b: bytes(b).decode().upper(),
    ) as store:
        key = store.put('a')
        assert store.get(key) == 'A'

        keys = store.put_batch(['b', 'c'])
        assert store.get_batch(keys) == ['B', 'C']

        key = store.connector.new_key()
        store._set(key, 'd')
        assert store.get(key) == 'D'


def test_custom_serializer_must_return_bytes() -> None:
    with Store(LocalConnector(), serializer=lambda s: s) as store:
        # Each fails because the list is not already serialized
        with pytest.raises(TypeError, match='bytes'):
            store.put([1, 2, 3])
        with pytest.raises(TypeError, match='bytes'):
            store.put_batch([[1, 2, 3]])
        with pytest.raises(TypeError, match='bytes'):
            store._set(store.connector.new_key(), [1, 2, 3])


def _deserialize_error(data: BytesLike) -> Any:
    raise ValueError('Oops')


def test_custom_deserializer_error() -> None:
    with Store(LocalConnector(), deserializer=_deserialize_error) as store:
        key = store.put('value')
        with pytest.raises(
            SerializationError,
            match='Failed to deserialize object',
        ):
            store.get(key)
        with pytest.raises(SerializationError, match='Failed to deserialize'):
            store.get_batch([key])


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


def test_put_batch(store: Store[LocalConnector]) -> None:
    values = ['test_value1', 'test_value2', 'test_value3']

    # Test without keys
    keys = store.put_batch(values)
    for key in keys:
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


@pytest.mark.parametrize('method', ('set_exception', 'set_result'))
def test_future_set_exception(
    method: str,
    store: Store[LocalConnector],
) -> None:
    future: ProxyFuture[str] = store.future()
    proxy = future.proxy()
    assert not future.done()

    getattr(future, method)(ValueError('Oops'))
    assert future.done()
    with pytest.raises(ValueError, match='Oops'):
        future.result()
    with pytest.raises(ProxyResolveError) as exc_info:
        resolve(proxy)
    assert isinstance(exc_info.value.cause, ValueError)


def test_proxy_of_exception_is_not_raised(
    store: Store[LocalConnector],
) -> None:
    # Only the result of a future is raised if it is an exception.
    proxy = store.proxy(ValueError('Oops'), populate_target=False)
    assert isinstance(proxy, ValueError)
    assert str(proxy) == 'Oops'


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


def test_get_batch_error_while_starting() -> None:
    with Store(LocalConnector()) as store:
        keys = [store.put('a'), store.put('b')]
        start = store.cache.start
        with (
            mock.patch.object(
                store.cache,
                'start',
                side_effect=[start(keys[0]), RuntimeError('start failed')],
            ),
            pytest.raises(RuntimeError, match='start failed'),
        ):
            store.get_batch(keys)

        # The key already started is not left pending.
        assert store.cache._pending == {}
        assert store.get_batch(keys) == ['a', 'b']


def test_store_get_batch_repeated_key_cache() -> None:
    with Store(LocalConnector(), cache_size=2) as store:
        key = store.put('a')
        assert store.get_batch([key, key]) == ['a', 'a']
        store.evict(key)
        for obj in 'bcd':
            assert store.get(store.put(obj)) == obj


def test_cache_mode_objects_shares_objects() -> None:
    with Store(LocalConnector()) as store:
        key = store.put([1, 2])
        assert store.get(key) is store.get(key)
        assert store.get_batch([key, key]) == [[1, 2], [1, 2]]


def test_cache_mode_bytes() -> None:
    with Store(LocalConnector(), cache_mode='bytes') as store:
        key = store.put([1, 2])
        missing = store.put('missing')
        store.evict(missing)

        first = store.get(key)
        assert store.is_cached(key)
        assert isinstance(store.cache.get(key), bytes)
        second = store.get(key)
        assert first == second == [1, 2]
        assert first is not second

        # Hits, misses, missing keys, and repeated keys each get their own
        # object.
        other = store.put([3])
        values = store.get_batch([key, other, other, missing], default='d')
        assert values == [[1, 2], [3], [3], 'd']
        assert len({id(v) for v in values[:3]}) == 3
        assert values[0] is not first
        assert store.get_batch([key]) == [[1, 2]]

        proxy1: Proxy[list[int]] = store.proxy_from_key(key)
        proxy2: Proxy[list[int]] = store.proxy_from_key(key)
        proxy1.append(3)
        assert proxy2 == [1, 2]


def test_cache_mode_bytes_deserializer_error() -> None:
    with Store(
        LocalConnector(),
        deserializer=_deserialize_error,
        cache_mode='bytes',
    ) as store:
        key = store.put('value')
        other = store.put('other')
        # Like the objects cache mode, data that cannot be deserialized is
        # not cached so the next get tries the connector again.
        with pytest.raises(SerializationError):
            store.get(key)
        assert not store.is_cached(key)
        with pytest.raises(SerializationError):
            store.get_batch([other])
        assert not store.is_cached(other)


def test_cache_mode_bytes_metrics() -> None:
    with Store(LocalConnector(), cache_mode='bytes', metrics=True) as store:
        key = store.put('value')
        assert store.get(key) == 'value'
        assert store.get(key) == 'value'
        assert store.get_batch([key]) == ['value']

        assert store.metrics is not None
        metrics = store.metrics.get_metrics(key)
        assert metrics is not None
        # Cache hits deserialize too, so they are timed.
        assert metrics.times['store.get.deserialize'].count == 2
        batch = store.metrics.get_metrics([key])
        assert batch is not None
        assert batch.times['store.get_batch.deserialize'].count == 1
        assert 'store.get_batch.connector' not in batch.times


def test_cache_mode_config() -> None:
    with Store(LocalConnector(), cache_mode='bytes') as store:
        config = store.config()
        assert config.cache_mode == 'bytes'
        with Store.from_config(config) as other:
            assert other._cache_mode == 'bytes'
