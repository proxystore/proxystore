# Dask Distributed with ProxyStore

*Last updated 24 April 2024*

This guide walks through using ProxyStore in [Dask Distributed](https://distributed.dask.org/){target=_blank}.
ProxyStore can be used to efficiently pass large intermediate values between function invocations.

!!! note

    Some familiarity with using Dask Distributed and ProxyStore is assumed.
    Check out the Dask Distributed [Quickstart](https://distributed.dask.org/en/stable/quickstart.html){target=_blank} and ProxyStore [Get Started](../get-started.md){target=_blank} to learn more.

## Installation

Create a new virtual environment of your choosing and install Dask Distributed and ProxyStore.

!!! note

    The below versions represent the latest versions of these packages
    available when this guide was written. These instructions should generally
    work with newer versions as well.

```bash
$ python -m venv venv
$ . venv/bin/activate
$ pip install dask[distributed]==2024.4.2 proxystore==0.6.5
```

## Using Dask Distributed

Dask Distributed is a library for futures-based distributed computing.
The [`Client.submit()`][distributed.Client.submit]{target=_blank} and [`Client.map()`][distributed.Client.map]{target=_blank} methods behave similarly to those of [`concurrent.futures.Executor`][concurrent.futures.Executor]{target=_blank}.
Consider this trivial example where we submit [`sum()`][sum]{target=_blank} on a list of numbers.

```python linenums="1" title="example.py"
from dask.distributed import Client

def main() -> None:
    client = Client(processes=True)

    x = list(range(100))
    y = client.submit(sum, x)
    print(f'Result: {y.result()}')

    client.close()

if __name__ == '__main__':
    main()
```

```bash
$ python example.py
Result: 4950
```

## Using ProxyStore

Dask Distributed has many builtin optimizations for data management when working with array-like data (e.g., NumPy arrays for Pandas dataframes).
However, other large objects can cause performance degradation when serialized along with the task graph.
ProxyStore provides a seamless alternative for passing objects to and from task invocations.

Here, we will modify the above example to use ProxyStore's [`FileConnector`][proxystore.connectors.file.FileConnector] to communicate intermediate data.
This example will work the same for any [`Connector`][proxystore.connectors.protocols.Connector] implementations, but different implementations can yield different performance benefits depending on the data or Dask Distributed deployment characteristics.

```python linenums="1" title="example.py" hl_lines="2 3 8 9 10 11 12 13 15"
from dask.distributed import Client
from proxystore.connectors.file import FileConnector
from proxystore.store import Store

def main() -> None:
    client = Client(processes=True)

    with Store(
        name='dask',
        connector=FileConnector('/tmp/proxystore-cache'),
        populate_target=True,  # (1)!
        register=True,  # (2)!
    ) as store:
        x = list(range(100))
        proxy = store.proxy(x)
        y = client.submit(sum, proxy)

        print(f'Result: {y.result()}')

    client.close()

if __name__ == '__main__':
    main()
```

1. Setting `populate_target=True` is always recommended with Dask Distributed.
2. Setting `register=True` is always recommended with Dask Distributed.

As expected, the result is the same.

```bash
$ python example.py
Result: 4950
```

Under the hood, ProxyStore is serializing `x` and putting the value in the connector.
The resulting `proxy` acts like a reference to the `x` that is now stored in a shared location.
The reference-like nature of `proxy` means that Dask does not end up serializing or transferring `x` itself; rather, Dask serializes the lightweight `proxy`.
The transparent nature of `proxy` means that when used by the task, `proxy` will resolve to and act like `x` ensuring that the functionality of the program is the exact same.

### Performance Tips

In the above example, we set two flags (`register` and `populate_target`) which will improve performance with ProxyStore in Dask Distributed applications.
Passing `#!python register=True` will call [`register_store()`][proxystore.store.base.Store] automatically to register the [`Store`][proxystore.store.base.Store] instance globally by name.
This enables proxies to reuse the same store instance, improving performance by sharing the same cache and stateful connections.

Most important for ProxyStore performance in Dask Distributed is `#!python populate_target=True`.
When `True`, created proxies will be "pre-resolved" and have their `__class__` and `__hash__` attributes cached inside the proxy.
This allows Dask to call [`hash()`][hash]{target=_blank} and [`isinstance()`][isinstance]{target=_blank} on a proxy without needing to resolve the proxy.
If `#!python populate_target=False` and we run the example with `DEBUG` level logging enabled, we will see that the target object of the proxy is retrieved three times.
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

```bash
$ python
INFO:proxystore.store:Registered a store named "dask"
INFO:proxystore.store.base:Initialized Store("dask", connector=FileConnector(directory=/tmp/proxystore-cache), serializer=default, deserializer=default, cache_size=16, metrics=False)
DEBUG:proxystore.store.base:Store(name="dask"): PUT FileKey(filename='3682883f-40bd-4990-bec0-73242f56067a') in 0.058 ms
DEBUG:proxystore.store.base:Store(name="dask"): PROXY FileKey(filename='3682883f-40bd-4990-bec0-73242f56067a') in 0.108 ms
DEBUG:proxystore.store.base:Store(name="dask"): GET FileKey(filename='3682883f-40bd-4990-bec0-73242f56067a') in 0.026 ms (cached=False)
DEBUG:proxystore.store.base:Store(name="dask"): GET FileKey(filename='3682883f-40bd-4990-bec0-73242f56067a') in 0.002 ms (cached=True)
INFO:proxystore.store:Registered a store named "dask"
INFO:proxystore.store.base:Initialized Store("dask", connector=FileConnector(directory=/tmp/proxystore-cache), serializer=default, deserializer=default, cache_size=16, metrics=False)
INFO:proxystore.store:Registered a store named "dask"
DEBUG:proxystore.store.base:Store(name="dask"): GET FileKey(filename='3682883f-40bd-4990-bec0-73242f56067a') in 0.034 ms (cached=False)
Result: 4950
INFO:proxystore.store:Unregistered a store named dask
```

Each `GET` message corresponds to an instance of `proxy` being resolved.
In this example, this happens (1) when the Dask client serializes `proxy`, (2) on the Dask scheduler when the task request message is processed, and (3) on the Dask worker when `proxy` is actually used in the computation.
If `x` was very large or costly to retrieve, this could significantly increase the application's memory usage or harmfully reduce task dispatch latency.
Running the example again with logging enabled but `#!python populate_target=True` will produce a single `GET` message corresponding to the Dask worker resolving `proxy` when the sum is computed which is optimal for performance.

### Memory Management

The [`Store`][proxystore.store.base.Store], by default, will not delete stored objects once they are no longer needed.
In the above example, this means that `x` will be stored in the [`FileConnector`][proxystore.connectors.file.FileConnector] until [`Store.close()`][proxystore.store.base.Store] is called and the directory `/tmp/proxystore-cache` is deleted.
(Here, [`Store.close()`][proxystore.store.base.Store] is called when exiting the `with` context block.)
However, it is not a requirement that [`Connector`][proxystore.connectors.protocols.Connector] implementations clear stored objects when closed.
In this case, the shared object `x` would be "leaked" because it was never deleted when no longer needed by the application.

ProxyStore provides many opt-in mechanisms for automated management of shared objects.
For single-use proxies, passing `#!python evict=True` to [`Store.proxy()`][proxystore.store.base.Store.proxy] will automatically delete the object from the store once the proxy is resolved.
In more complex scenarios where a proxy may be used by many processes, [Lifetimes][proxystore.store.lifetimes] or [Ownership][proxystore.store.ref] can be used.
Check out the [Object Lifetimes](object-lifetimes.md) guide to learn more.
