# Proxy Futures

*Last updated 1 November 2023*

This guide walks through the use of the
[`Store.future()`][proxystore.store.base.Store.future] interface and associated
[`ProxyFuture`][proxystore.store.future.ProxyFuture].

!!! note

    Some familiarity with ProxyStore is assumed. Check out the
    [Get Started](../get-started.md){target=_blank} guide and
    [Concepts](../concepts/index.md){target=_blank} page to learn more about
    ProxyStore's core concepts.

!!! warning

    The [`Store.future()`][proxystore.store.base.Store.future] and
    [`ProxyFuture`][proxystore.store.future.ProxyFuture] interfaces are
    experimental features and may change in future releases.

The [`ProxyFuture`][proxystore.store.future.ProxyFuture] interface enables
a data producer to preemptively send a proxy to a data consumer before the
target data has been created. The consumer of the target data proxy will
block when the proxy is first used and resolved until the producer
has created the target data.

Here is a trivial example using a [`Store`][proxystore.store.base.Store] and
[`LocalConnector`][proxystore.connectors.local.LocalConnector]. The
[`future.proxy()`][proxystore.store.future.ProxyFuture.proxy] method is used
to create a [`Proxy`][proxystore.proxy.Proxy] which will resolve to the
result of the future.

```python linenums="1" title="example.py"
from proxystore.connectors.local import LocalConnector
from proxystore.store import Store
from proxystore.store.future import ProxyFuture

with Store('proxy-future-example', LocalConnector()) as store:
    future: ProxyFuture[str] = store.future()
    proxy = future.proxy()

    future.set_result('value')
    assert future.result() == 'value'
    assert proxy == 'value'
```

!!! info

    Not all [`Connector`][proxystore.connectors.protocols.Connector]
    implementations are compatible with the
    [`Store.future()`][proxystore.store.base.Store.future] interface.
    The [`Connector`][proxystore.connectors.protocols.Connector] instance used
    to initialize the [`Store`][proxystore.store.base.Store] must also
    implement the
    [`DeferrableConnector`][proxystore.connectors.protocols.DeferrableConnector]
    protocol. A [`NotImplementedError`][NotImplementedError] will be
    raised when calling [`Store.future()`][proxystore.store.base.Store.future]
    if the connector is not an instance of
    [`DeferrableConnector`][proxystore.connectors.protocols.DeferrableConnector].
    Many of the out-of-the-box implementations implement the
    [`DeferrableConnector`][proxystore.connectors.protocols.DeferrableConnector]
    protocol such as the
    [`EndpointConnector`][proxystore.connectors.endpoint.EndpointConnector],
    [`FileConnector`][proxystore.connectors.file.FileConnector], and
    [`RedisConnector`][proxystore.connectors.redis.RedisConnector].

The power of [`ProxyFuture`][proxystore.store.future.ProxyFuture] comes when
the data producer and consumer are executing independently in time and space
(i.e., execution occurs in different processes, potentially on different
systems, and in an undefined order). The
[`ProxyFuture`][proxystore.store.future.ProxyFuture] enables the producer
and consumer to share a data dependency, while allowing the consumer to
eagerly start execution before the data dependencies are fully satisfied.

Consider the following example where we have a client which invokes two
functions, `foo()` and `bar()` on remote processes. `foo()` will produce an
object needed by `bar()`, but we want to start executing `foo()` and `bar()`
at the same time. (We could even start `bar()` before `foo()`!)

```python linenums="1" title="client.py"
from proxystore.connectors.redis import RedisConnector
from proxystore.store import Store
from proxystore.store.future import ProxyFuture

class MyData:
    ...

def foo(future: ProxyFuture[MyData]) -> None:
    data: MyData = compute(...)
    future.set_result(data)

def bar(data: MyData) -> None:
    # Computation not involving data can execute freely.
    compute(...)
    # Computation using data will block until foo
    # sets the result of the future.
    compute(data)


with Store('proxy-future-example', RedisConnector(...)) as store:
    future: ProxyFuture[MyData] = store.future()

    # The invoke_remote function will execute the function with
    # the provided on arguments on an arbitrary remote process.
    foo_result_future = invoke_remote(foo, future)
    bar_result_future = invoke_remote(bar, future.proxy())

    # Wait on the functions to finish executing.
    foo_result_future.result()
    bar_result_future.result()
```

In this example, `foo()` and `bar()` started executing at the same time.
This allows `bar()` to eagerly execute code which does not depend on the
data produced by `foo()`. `bar()` will only block once the data is needed by
the computation.
