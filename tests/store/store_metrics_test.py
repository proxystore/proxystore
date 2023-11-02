from __future__ import annotations

import pathlib
from typing import Generator

import pytest

from proxystore.connectors.file import FileConnector
from proxystore.serialize import serialize
from proxystore.store import store_registration
from proxystore.store.base import Store


@pytest.fixture()
def store(
    tmp_path: pathlib.Path,
) -> Generator[Store[FileConnector], None, None]:
    # We use FileConnector here instead of LocalConnector (as in the rest of
    # the tests) because FileConnector operations take *some* amount of time.
    path = str(tmp_path)
    with Store('test', connector=FileConnector(path), metrics=True) as store:
        with store_registration(store):
            yield store


def test_store_single_key_operations(store: Store[FileConnector]) -> None:
    value = 'value'
    key = store.put(value)
    assert store.exists(key)
    assert store.get(key) == value
    assert store.get(key) == value
    assert store.exists(key)
    store._set(key, value)
    store.evict(key)

    assert store.metrics is not None
    key_metrics = store.metrics.get_metrics(key)
    assert key_metrics is not None

    size = len(serialize(value))
    assert key_metrics.attributes['store.get.object_size'] == size
    assert key_metrics.attributes['store.put.object_size'] == size
    assert key_metrics.attributes['store.set.object_size'] == size

    assert key_metrics.counters['store.get.cache_hits'] == 1
    assert key_metrics.counters['store.get.cache_misses'] == 1

    assert key_metrics.times['store.exists'].count == 2
    assert key_metrics.times['store.exists.connector'].count == 1

    assert key_metrics.times['store.evict'].count == 1
    assert key_metrics.times['store.evict.connector'].count == 1

    assert key_metrics.times['store.get'].count == 2
    assert key_metrics.times['store.get.connector'].count == 1
    assert key_metrics.times['store.get.deserialize'].count == 1

    assert key_metrics.times['store.put'].count == 1
    assert key_metrics.times['store.put.connector'].count == 1
    assert key_metrics.times['store.put.serialize'].count == 1

    assert key_metrics.times['store.set'].count == 1
    assert key_metrics.times['store.set.connector'].count == 1
    assert key_metrics.times['store.put.serialize'].count == 1

    proxy = store.proxy(value)
    assert proxy == value

    proxy_metrics = store.metrics.get_metrics(proxy)
    assert proxy_metrics is not None

    assert proxy_metrics.times['store.proxy'].count == 1
    assert proxy_metrics.times['factory.call'].count == 1
    assert proxy_metrics.times['factory.resolve'].count == 1


def test_store_multi_key_operations(store: Store[FileConnector]) -> None:
    values = ['value1', 'value2', 'value3']
    keys = store.put_batch(values)
    assert all(store.exists(key) for key in keys)

    assert store.metrics is not None
    key_metrics = store.metrics.get_metrics(keys)
    assert key_metrics is not None

    sizes = sum(len(serialize(value)) for value in values)
    assert key_metrics.attributes['store.put_batch.object_sizes'] == sizes
    assert key_metrics.times['store.put_batch.serialize'].count == 1
    assert key_metrics.times['store.put_batch.connector'].count == 1
    assert key_metrics.times['store.put_batch'].count == 1

    proxies = store.proxy_batch(values)
    for proxy, value in zip(proxies, values):
        assert proxy == value

    proxy_metrics = store.metrics.get_metrics(proxies)
    assert proxy_metrics is not None

    assert proxy_metrics.times['store.put_batch'].count == 1
    assert proxy_metrics.times['store.proxy_batch'].count == 1

    for proxy in proxies:
        proxy_metrics = store.metrics.get_metrics(proxy)
        assert proxy_metrics is not None

        assert proxy_metrics.times['store.get'].count == 1
        assert proxy_metrics.times['factory.call'].count == 1
        assert proxy_metrics.times['factory.resolve'].count == 1
