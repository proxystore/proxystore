# Globus Compute with ProxyStore

*Last updated 2 May 2023*

This guide walks through integrating ProxyStore into a
[Globus Compute](https://www.globus.org/compute){target=_blank} application.
A more complete example of using ProxyStore with Globus Compute can be found
in the [`examples/`](https://github.com/proxystore/proxystore/blob/main/examples){target=_blank}.

!!! note

    Some familiarity with using Globus Compute and ProxyStore is assumed.
    Check out the Globus Compute
    [Quickstart](https://globus-compute.readthedocs.io/en/latest/quickstart.html){target=_blank}
    and ProxyStore [Get Started](../get-started.md){target=_blank}
    to learn more.

## Installation

Create a new virtual environment of your choosing and install Globus Compute
and ProxyStore.

!!! note

    The below versions represent the latest versions of these packages
    available when this guide was written. These instructions should generally
    work with newer versions as well.

```bash
$ python -m venv venv
$ . venv/bin/activate
$ pip install globus-compute-sdk==2.0.1 globus-compute-endpoint==2.0.1 proxystore==0.5.*
```

## Using Globus Compute

We will first configure and start a Globus Compute endpoint.

```bash
$ globus-compute-endpoint configure proxystore-example
$ globus-compute-endpoint start proxystore-example
```

After configuring the endpoint, you will get back an endpoint UUID which we
will need in the next step.

Below is a modified example based on the example Globus Compute app from the
[Quickstart](https://globus-compute.readthedocs.io/en/latest/quickstart.html){target=_blank}
guide.
```python linenums="1" title="example.py"
from globus_compute_sdk import Executor

ENDPOINT_UUID = '5b994a7d-8d7c-48d1-baa1-0fda09ea1687' # (1)!

def average(x: list[float]) -> float:  # (2)!
    return sum(x) / len(x)

with Executor(endpoint_id=ENDPOINT_UUID) as gce:  # (3)!
    x = list(range(1, 100000))
    future = gce.submit(average, x)  # (4)!

    print(future.result())  # (5)!
```

1. Your endpoint's UUID.
2. Define the function that will be executed remotely.
3. Create the Globus Compute executor.
4. Submit the function for execution.
5. Wait on the result future.

Running this script will return `50000`.
```bash
$ python example.py
50000.0
```

## Using ProxyStore

Now we will update our script to use ProxyStore. This takes three steps:

1. Initialize a [`Connector`][proxystore.connectors.protocols.Connector] and
   [`Store`][proxystore.store.base.Store]. The `Connector` is the interface
   to the byte-level communication channel that will be used, and the `Store`
   is the high-level interface provided by ProxyStore.
2. Register the `Store` instance globally. This is not strictly necessary, but
   is an optimization which enables proxies to share the same original `Store`
   instance, because the `Store` and `Connector` can have state (e.g., caches,
   open connections, etc.).
3. Proxy the function inputs.

```python linenums="1" title="example.py" hl_lines="2 3 4 11 12 16 21"
from globus_compute_sdk import Executor
from proxystore.connectors.file import FileConnector
from proxystore.store import register_store
from proxystore.store import Store

ENDPOINT_UUID = '5b994a7d-8d7c-48d1-baa1-0fda09ea1687'

def average(x: list[float]) -> float:
    return sum(x) / len(x)

store = Store('my-store', FileConnector('./proxystore-cache'))  # (1)!
register_store(store) # (2)!

with Executor(endpoint_id=ENDPOINT_UUID) as gce:
    x = list(range(1, 100000))
    p = store.proxy(x) # (3)!
    future = gce.submit(average, p)

    print(future.result())

store.close() # (4)!
```

1. Create a new store using the file system for mediated communication.
2. Register the store instance so states (e.g., caches, etc.) can be shared.
3. Proxy the input data.
4. Close the `Store` to cleanup any resources.

!!! tip

    The [`Store`][proxystore.store.base.Store] can also be used as a context
    manager that will automatically clean up resources.

    ```python
    with Store('my-store', FileConnector('./proxystore-cache')) as store:
        x = list(range(1, 100000))
        p = store.proxy(x)
        future = gce.submit(average, p)

        print(future.result())
    ```

We can also use ProxyStore to return data via the same communication method.

```python linenums="1" title="example.py" hl_lines="2 3 7 8 9"
def average(x: list[float]) -> float:
    from proxystore.proxy import Proxy # (1)!
    from proxystore.store import get_store

    avg = sum(x) / len(x)

    if isinstance(x, Proxy): # (2)!
        store = get_store(x)
        avg = store.proxy(avg)

    return avg
```

1. Globus Compute functions will be executed in a different process so we must
   import inside the function.
2. If our input data was communicated via a proxy, we get the same `Store` that
   create our input proxy which we then use to proxy the output.

## Closing Thoughts

While this example is trivial, the target function is still executed on
the local machine and the data sizes are small, the key takeaway is that
the [`Proxy`][proxystore.proxy.Proxy] model simplifies the process of moving
data via alternate means between the Globus Compute client and executors.

More complex applications where the Globus Compute endpoints live elsewhere
(e.g., on an HPC) cluster or that move larger data will benefit from the
various [`Connector`][proxystore.connectors.protocols.Connector]
implementations provided.

Checkout the other [Guides](index.md) to learn about more advanced ProxyStore
features.
