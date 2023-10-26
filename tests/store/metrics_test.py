from __future__ import annotations

from typing import Any

from proxystore.proxy import Proxy
from proxystore.store.factory import StoreFactory
from proxystore.store.metrics import Metrics
from proxystore.store.metrics import StoreMetrics
from proxystore.store.metrics import TimeStats


def test_time_stats() -> None:
    stats1 = TimeStats()
    stats1.add_time(1)
    stats2 = TimeStats()
    stats2.add_time(3)
    stats3 = stats1 + stats2

    assert stats3.count == 2
    assert stats3.avg_time_ms == 2
    assert stats3.min_time_ms == 1
    assert stats3.max_time_ms == 3
    assert stats3.last_time_ms == 3
    assert stats3.last_timestamp == stats2.last_timestamp
    assert stats3.last_timestamp > stats1.last_timestamp

    stats3.add_time(6)
    assert stats3.last_time_ms == 6
    assert stats3.last_timestamp > stats2.last_timestamp

    stats4 = stats3 + TimeStats()
    assert stats3 == stats4


def test_time_stats_as_dict() -> None:
    stats = TimeStats(count=1, avg_time_ms=1, min_time_ms=1, max_time_ms=1)
    stats_dict = stats.as_dict()
    assert stats == TimeStats(**stats_dict)


def test_metrics_as_dict() -> None:
    stats = TimeStats(count=1, avg_time_ms=1, min_time_ms=1, max_time_ms=1)
    attributes = {'attr1': 'value1', 'attr2': 2}
    counters = {'counter1': 1}
    times = {'time1': TimeStats(), 'time2': stats}

    metrics = Metrics(attributes=attributes, counters=counters, times=times)
    metrics_dict = metrics.as_dict()
    for key, value in metrics_dict['times'].items():
        metrics_dict['times'][key] = TimeStats(**value)
    assert metrics == Metrics(**metrics_dict)


def test_store_metrics_by_key() -> None:
    metrics = StoreMetrics()
    # Test both single keys and tuples of keys because the Store
    # tracks metrics for single key operations and batch key operations
    keys: list[tuple[str, ...] | list[tuple[str, ...]]] = [
        ('test-key',),
        [('key1',), ('key2',), ('key3',)],
    ]

    for key in keys:
        metrics.add_attribute('test-attribute', key, 'value')
        metrics.add_counter('test-counter', key, 1)
        metrics.add_counter('test-counter', key, 1)
        metrics.add_time('test-timer', key, 1000)
        metrics.add_time('test-timer', key, 2000)

        key_metrics = metrics.get_metrics(key)
        assert key_metrics is not None

        assert key_metrics.attributes == {'test-attribute': 'value'}
        assert key_metrics.counters == {'test-counter': 2}
        assert key_metrics.times['test-timer'].count == 2
        assert key_metrics.times['test-timer'].avg_time_ms == 1.5
        assert key_metrics.times['test-timer'].min_time_ms == 1
        assert key_metrics.times['test-timer'].max_time_ms == 2


def test_store_metrics_by_missing_key() -> None:
    metrics = StoreMetrics()
    assert metrics.get_metrics(('test-key',)) is None


def test_metrics_by_proxy() -> None:
    metrics = StoreMetrics()

    key = ('test-key',)
    proxy: Proxy[Any] = Proxy(StoreFactory(key, {}))

    metrics.add_attribute('test-attribute', key, 'value')
    assert metrics.get_metrics(proxy) == metrics.get_metrics(key)


def test_metrics_by_proxies() -> None:
    metrics = StoreMetrics()

    keys = [('key1',), ('key2',), ('key3',)]
    proxies: list[Proxy[Any]] = [Proxy(StoreFactory(key, {})) for key in keys]

    metrics.add_attribute('test-attribute', proxies, 'value')
    assert metrics.get_metrics(proxies) == metrics.get_metrics(keys)


def test_store_metrics_aggregate_times() -> None:
    metrics = StoreMetrics()
    keys = [('key1',), ('key2',), ('key3',)]

    for i, key in enumerate(keys):
        metrics.add_time('time1', key, (1 + i) * 1000)
        metrics.add_time('time2', key, (1 + i) * 10000)

    times = metrics.aggregate_times()
    assert times['time1'].count == len(keys)
    assert times['time1'].avg_time_ms == 2
    assert times['time1'].min_time_ms == 1
    assert times['time1'].max_time_ms == 3
    assert times['time2'].count == len(keys)
    assert times['time2'].avg_time_ms == 20
    assert times['time2'].min_time_ms == 10
    assert times['time2'].max_time_ms == 30
