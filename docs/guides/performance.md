# Performance Tracking

The ProxyStore [`Store`][proxystore.store.base.Store] interface provides low-level performance tracking on store operations (e.g., `get` and `set`).
Performance tracking is disable by default and can be enabled by passing `#!python stats=True` to a [`Store`][proxystore.store.base.Store] constructor.

```python
from proxystore.store.file import FileStore

store = FileStore(
   name="default",
   store_dir="/tmp/proxystore-dump",
   stats=True,
)
```

Performance statistics are aggregated on a per-key level and can be accessed via the [`Store.stats()`][proxystore.store.base.Store.stats] method.
[`Store.stats()`][proxystore.store.base.Store.stats] takes a key (string) or [`Proxy`][proxystore.proxy.Proxy] and returns a dictionary mapping [`Store`][proxystore.store.base.Store] operations to [`TimeStats`][proxystore.store.stats.TimeStats] objects containing the aggregated statistics for the operation.

!!! warning
    Performance statistics are local to each [`Store`][proxystore.store.base.Store] instance.
    In multi-process applications or applications that instantiate multiple [`Store`][proxystore.store.base.Store] instances, the statistics returned by [`Store.stats()`][proxystore.store.base.Store.stats] will only represent a partial view of the overall performance.

!!! warning
    Attempting to query [`Store.stats()`][proxystore.store.base.Store.stats] without initializing the store to track stats will raise a `#!python ValueError`.

Continuing with the above `store` object, an instance of [`FileStore`][proxystore.store.file.FileStore] configured to track performance statistics, we can perform operations on `store` and inspect the statistics.
In the following block, we add an object to the store and see that there are now performance statistics on the [`FileStore.set()`][proxystore.store.base.Store.set] and [`FileStore.set_bytes()`][proxystore.store.base.Store.set_bytes] operations.

```python
target = list(range(0, 100))
key = store.set(target)

stats = store.stats(key)
stats.keys()
# >>> dict_keys(['set_bytes', 'set'])
stats['set']
# >>> TimeStats(
# >>>     calls=1,
# >>>     avg_time_ms=0.0686,
# >>>     min_time_ms=0.0686,
# >>>     max_time_ms=0.0686,
# >>>     size_bytes=None,
# >>> )
stats['set_bytes']
# >>> TimeStats(
# >>>     calls=1,
# >>>     avg_time_ms=0.0339,
# >>>     min_time_ms=0.0339,
# >>>     max_time_ms=0.0339,
# >>>     size_bytes=219,
# >>> )
```

Operations that work directly on bytes (i.e., `#!python get_bytes()` and
`#!python set_bytes()`) will also note the size of the byte array used in the
operation in the `#!python TimeStats.size_bytes` attribute.
As more operations are performed on the store, more statistics will be accumulated.

```python
target = store.get(key)
stats = store.stats(key)

stats.keys()
# >>> dict_keys(
# >>>     ['set_bytes', 'set', 'is_cached', 'get_bytes', 'exists', 'get']
# >>> )

# Attributes of `TimeStats` can be accessed directly
stats['get'].calls
# >>> 1
stats['get'].avg_time_ms
# >>> 0.0625

# Check that the avg time of `get` decreases due to caching
# when called twice in a row.
target = store.get(key)
stats = store.stats(key)
stats['get'].calls
# >>> 2
stats['get'].avg_time_ms
# >>> 0.0376
```

Performance statistics can also be accessed with a proxy.

```python
target_proxy = store.proxy(target)
stats = store.stats(target_proxy)
stats.keys()
# >>> dict_keys(['set_bytes', 'set', 'proxy'])
stats['proxy'].avg_time_ms
# >>> 0.0691
```

Proxies produced by a store with performance tracking enabled will also track statistics on time taken to resolve itself.
When [`Store.stats()`][proxystore.store.base.Store.stats] is passed a proxy, the method will inspect the proxy for any performance statistics and include any statistics in the result.

```python
# Access the proxy to force it to resolve.
target_proxy[0:5]
# >>> [0, 1, 2, 3, 4]

stats = store.stats(target_proxy)
stats.keys()
# >>> dict_keys(
# >>>     ['resolve', 'set_bytes', 'set', 'proxy',
# >>>      'is_cached', 'get_bytes', 'exists', 'get']
# >>> )
stats['resolve'].avg_time_ms
# >>> 0.0587
```

Python code used to generate the above examples can be found at https://github.com/proxystore/proxystore/blob/main/examples/store_stats.py.
