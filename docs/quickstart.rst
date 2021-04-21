Quick Start
###########

ProxyStore is a package designed to simplify the use of remote object stores for transferring large Python objects, particularly in distributed environments.

Overview
--------

ProxyStore is based on lazy object proxies.
A `proxy` is an object that wraps another object (referred to as the `wrapped object`) and can intercept and redefine functionality of the wrapped object.
A proxy that the does not redefine and functionality will behave just as the wrapped object would.

Lazy proxies, rather than wrapping an object directly, wrap a function which when called produces the actually object (referred to as the `factory`).
A lazy proxy delays executing the factory until the first time the proxy is used.
As a result, the lazy proxy is just a skeleton than can be passed around until needed.
The process of calling the factory is referred to as `resolving` the proxy.

ProxyStore uses these lazy proxies to efficiently pass objects between Python processes or physical devices via remote object stores.
ProxyStore has three main components: 1) the Proxy, 2) a set of useful factories, and 3) a unified interface to backend object-stores.

ProxyStore currently provides support for local memory store and `Redis <https://redis.io/>`_ stores.

Installation
------------

.. code-block:: bash

   $ pip install ProxyStore

ProxyStore only requires installing the bare-minimum packages, and may require additional packages to be installed depending on which backends are used (e.g., `redis-py <https://redis-py.readthedocs.io/en/stable/>`_ is required for the Redis backend).

Documentation on installing for local development is provided in :doc:`Contributing <./contributing>`.

Usage
-----

ProxyStore exposes two primary methods for passing objects with proxies:
a method to initialize the backend and a method to proxy an object.

.. code-block:: python

   import numpy as np
   import proxystore as ps

   x = np.array([1, 2, 3])

   ps.init_redis_backend(hostname=REDIS_HOST, port=REDIS_PORT)
   p = ps.to_proxy(x)

In this case, we have initialized a backend connected to a Redis server that is already running on `REDIS_HOST:REDIS_PORT`.
Then, we proxied our numpy array.
The process of proxying involves placing the array in the Redis server, creating a factory that can resolve the array when called, then creating a proxy with the factory.

The proxy `p` now acts as a thin reference to the array which lives in the Redis server.
This proxy can be cheaply serialized and sent to other processes.
When `p` is accessed for the first time, say because :code:`np.sum(p)` is called on it, the factory is called and the array is retrieved from Redis.
Now for the rest of its existence, `p` will just behave as `x` would.

See :doc:`Advanced Usage <./advanced>` for more detailed functionality in ProxyStore.
