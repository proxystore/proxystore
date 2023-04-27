"""Store metrics example.

Source code for:
    https://proxystore.readthedocs.io/en/latest/guides/performance.html
"""
from __future__ import annotations

import dataclasses
import tempfile
from pprint import pprint

from proxystore.connectors.file import FileConnector
from proxystore.store import register_store
from proxystore.store.base import Store

fp = tempfile.TemporaryDirectory()

store = Store('example', FileConnector(fp.name), metrics=True)
register_store(store)
assert store.metrics is not None

target = list(range(0, 100))
key = store.put(target)
store.get(key)

metrics = store.metrics.get_metrics(key)
assert metrics is not None
attrs = tuple(field.name for field in dataclasses.fields(metrics))
print(f'Metrics attributes: {attrs}')
print('Attributes:')
pprint(metrics.attributes)
print('Counters:')
pprint(metrics.counters)
print('Times:')
print(metrics.times)

store.get(key)
metrics = store.metrics.get_metrics(key)
assert metrics is not None
print('Counters:')
pprint(metrics.counters)
print('Get Time:')
pprint(metrics.times['store.get'])
print(f'Access by attribute: {metrics.times["store.get"].avg_time_ms}')

proxy = store.proxy(target)
# Force proxy to resolve
assert proxy[0] == 0

metrics = store.metrics.get_metrics(proxy)
assert metrics is not None
print('Times:')
print(metrics.times)

keys = store.put_batch(['value1', 'value2', 'value3'])
metrics = store.metrics.get_metrics(keys)
assert metrics is not None
print('Times:')
print(metrics.times)

times = store.metrics.aggregate_times()
print('Aggregate times:')
pprint(times)

store.close()
fp.cleanup()
