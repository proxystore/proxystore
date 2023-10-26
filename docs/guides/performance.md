# Performance Tracking

*Last updated 2 May 2023*

The [`Store`][proxystore.store.base.Store] can record metrics on executed operations (e.g., `get` and `put`).
Metric collection is disabled by default and can be enabled by passing `#!python metrics=True` to a [`Store`][proxystore.store.base.Store] constructor.

## Enabling Metrics

```python linenums="1"
import dataclasses
from proxystore.connectors.file import FileConnector
from proxystore.store import register_store
from proxystore.store.base import Store

store = Store(
   name='example-store',
   connector=FileConnector('/tmp/proxystore-dump'),
   metrics=True,  # (1)!
)
register_store(store)
assert store.metrics is not None
```

1. Metric tracking is not enabled by default.

Metrics are accessed via the
[`Store.metrics`][proxystore.store.base.Store.metrics] property. This property
will be `None` when metrics are disabled.

!!! warning
    Metrics are local to each [`Store`][proxystore.store.base.Store] instance.
    In multi-process applications or applications that instantiate multiple
    [`Store`][proxystore.store.base.Store] instances,
    [`Store.metrics`][proxystore.store.base.Store.metrics] will only represent
    a partial view of the overall performance.

Three types of metrics are collected.

* Attributes: arbitrary attributes associated with an operation.
* Counters: scalar counters that represent the number of times an event occurs.
* Times: durations of events.

## A Simple Example

Consider executing a `get` and `put` operation on `store`.
```python
>>> key = store.put([0, 1, 2, 3, 4, 5])
>>> store.get(key)
```

We can inspect the metrics recorded for operations on `key`.
```python
>>> metrics = store.metrics.get_metrics(key)

>>> tuple(field.name for field in dataclasses.fields(metrics))
('attributes', 'counters', 'times')
```

`metrics` is an instance of [`Metrics`][proxystore.store.metrics.Metrics] which
is a [`dataclass`][dataclasses.dataclass] with three fields:
`attributes`, `counters`, and `times`. We can further inspect these fields.
```python
>>> metrics.attributes
{'store.get.object_size': 219, 'store.put.object_size': 219}
>>> metrics.counters
{'store.get.cache_misses': 1}
>>> metrics.times
{
    'store.put.serialize': TimeStats(
        count=1, avg_time_ms=9.9, min_time_ms=9.9, max_time_ms=9.9
    ),
    'store.put.connector': TimeStats(
       count=1, avg_time_ms=36.9, min_time_ms=36.9, max_time_ms=36.9
    ),
    'store.put': TimeStats(
       count=1, avg_time_ms=53.4, min_time_ms=53.4, max_time_ms=53.4
    ),
    'store.get.connector': TimeStats(
       count=1, avg_time_ms=16.1, min_time_ms=16.1, max_time_ms=16.1
    ),
    'store.get.deserialize': TimeStats(
       count=1, avg_time_ms=7.6, min_time_ms=7.6, max_time_ms=7.6
    ),
   'store.get': TimeStats(
       count=1, avg_time_ms=45.6, min_time_ms=45.6, max_time_ms=45.6
   ),
}
```

Operations or events are represented by a hierarchical namespace.
E.g., `store.get.object_size` is the serialized object size from the call to
[`Store.get()`][proxystore.store.base.Store.get].
In `metrics.attributes`, we see the serialized object was 219 bytes.
In `metrics.counters`, we see we had one cache miss when getting the object.
In `metrics.times`, we see statistics about the duration of each operation.
For example, `store.get` is the overall time
[`Store.get()`][proxystore.store.base.Store.get] took, `store.get.connector` is
the time spent calling
[`Connector.get()`][proxystore.connectors.protocols.Connector.get], and
`store.get.deserialize` is the time spent deserializing the object returned
by [`Connector.get()`][proxystore.connectors.protocols.Connector.get].

If we get the object again, we'll see the metrics change.
```python
>>> store.get(key)
>>> metrics = store.metrics.get_metrics(key)
>>> metrics.counters
{'store.get.cache_hits': 1, 'store.get.cache_misses': 1}
>>> metrics.times['store.get']
TimeStats(count=2, avg_time_ms=24.4, min_time_ms=3.2, max_time_ms=45.6)
```
Here, we see that the second get resulted in a cache hit, and our average
time for `store.get` dropped significantly.

Attributes of a [`TimeStats`][proxystore.store.metrics.TimeStats] instance
can be directly accessed.
```python
>>> metrics.times['store.get'].avg_time_ms
24.4
```

## Metrics with Proxies

Metrics are also tracked on proxy operations.
```python
>>> proxy = store.proxy(target)

# Access the proxy to force it to resolve.
>>> assert target_proxy[0] == 0

>>> metrics = store.metrics.get_metrics(proxy)
>>> metrics.times
{
    'factory.call': TimeStats(...)
    'factory.resolve': TimeStats(...),
    'store.get': TimeStats(...),
    'store.get.connector': TimeStats(...),
    'store.get.deserialize': TimeStats(...),
    'store.proxy': TimeStats(...),
    'store.put': TimeStats(...),
    'store.put.connector': TimeStats(...),
    'store.put.serialize': TimeStats(...),
}
```
Calling [`Store.proxy()`][proxystore.store.base.Store.proxy] internally
called [`Store.put()`][proxystore.store.base.Store.put]. Accessing the
proxy internally resolved the factory so we also see metrics about the
`factory` and `store.get`.

!!! warning

    For metrics to appropriately be tracked when a proxy is resolved, the
    [`Store`][proxystore.store.base.Store] needs to be registered globally
    with [`register_store()`][proxystore.store.register_store]. Otherwise,
    the factory will initialize a second [`Store`][proxystore.store.base.Store]
    to register and record its metrics to the second instance.

## Metrics for Batch Operations

For batch [`Store`][proxystore.store.base.Store] operations, metrics are
recorded for the entire batch. I.e., the batch of keys is treated as a single
super key.

```python
>>> keys = store.put_batch(['value1', 'value2', 'value3'])
>>> metrics = store.metrics.get_metrics(keys)
>>> metrics.times
{
    'store.put_batch.serialize': TimeStats(...),
    'store.put_batch.connector': TimeStats(...),
    'store.put_batch': TimeStats(...)
}
```

## Aggregating Metrics

Rather than accessing metrics associated with a specific key (or batched key),
time statistics can be aggregated over all keys.

```python
>>> store.metrics.aggregate_times()
{
    'factory.call': TimeStats(...),
    'factory.resolve': TimeStats(...),
    'store.get': TimeStats(...),
    'store.get.connector': TimeStats(...),
    'store.get.deserialize': TimeStats(...),
    'store.proxy': TimeStats(...),
    'store.put': TimeStats(...),
    'store.put.connector': TimeStats(...),
    'store.put.serialize': TimeStats(...),
    'store.put_batch': TimeStats(...),
    'store.put_batch.connector': TimeStats(...),
    'store.put_batch.serialize': TimeStats(...),
}
```
Each of these [`TimeStats`][proxystore.store.metrics.TimeStats] represents
the aggregate over all keys.

The Python code used to generate the above examples can be found at
[github.com/proxystore/proxystore/examples/store_metrics.py](https://github.com/proxystore/proxystore/blob/main/examples/store_metrics.py){target=_blank}.
