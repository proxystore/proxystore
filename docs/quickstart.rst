Quick Start
###########

ProxyStore is a package designed to simplify the use of remote object stores for transferring large Python objects, particularly in distributed environments.
The goal of ProxyStore is to simplify the process of sending objects from a source and remove the need for any changes at the destination.

Overview
--------

ProxyStore is based on lazy object proxies.
A `proxy` is an object that wraps another object (referred to as the `wrapped object`) and can intercept and redefine functionality of the wrapped object.
A proxy that the does not redefine any functionality will behave just as the wrapped object would.

Lazy proxies, rather than wrapping an object directly, wrap a function which when called produces the actually object (referred to as the `factory`).
A lazy proxy delays executing the factory until the first time the proxy is used.
As a result, the lazy proxy is just a skeleton than can be passed around until needed.
The process of calling the factory is referred to as `resolving` the proxy, and proxies perform just-in-time resolution.

ProxyStore provides the :any:`Proxy <proxystore.proxy.Proxy>` and :any:`Factory <proxystore.factory.Factory>` building blocks for creating powerful just-in-time resolution functionality for Python objects.

ProxyStore also provides implementations for interacting with remote object stores via proxies.
Passing objects to remote machines via proxies is powerful because all the functionality for resolving the object is package with the proxy and the destination code does not need to be modified to handle the proxies.

Installation
------------

.. code-block:: bash

   $ pip install proxystore

ProxyStore only installs the bare-minimum dependencies.
Additional packages may need to be installed depending on which object store interfaces are used (e.g., `redis-py <https://redis-py.readthedocs.io/en/stable/>`_ is required for interacting with Redis servers).

Documentation on installing for local development is provided in :doc:`Contributing <./contributing>`.

Usage
-----

Proxies can be created easily using ProxyStore.

.. code-block:: python

   import proxystore as ps

   def resolve_object(...):
       # Function that produces the object of interest
       return obj

   p = ps.proxy.Proxy(ps.factory.LambdaFactory(resolve_object))

:code:`resolve_object()` will be called when the proxy :code:`p` does its just-in-time resolution, and then :code:`p` will behave exactly like :code:`obj`.
The :any:`LambdaFactory <proxystore.factory.LambdaFactory>` accepts any callable Python object (functions, lambdas, etc.).

ProxyStore provides a :any:`Store <proxystore.store.base.Store>` interface for interacting with object stores.
For example, proxies can be created for objects in a Redis server easily.

.. code-block:: python

   import numpy as np
   import proxystore as ps

   x = np.array([1, 2, 3])

   store = ps.store.init_store(
       'redis', name='redis', hostname=REDIS_HOST, port=REDIS_PORT
   )
   p = store.proxy(x)

A :any:`RedisStore <proxystore.store.redis.RedisStore>` interface is initialized to connect to a Redis server hosted at :code:`REDIS_HOST:REDIS_PORT`.
The interface exposes standard :any:`get() <proxystore.store.redis.RedisStore.get>` and :any:`set() <proxystore.store.redis.RedisStore.set>` functionality for interacting with the remote Redis server.

:any:`proxy() <proxystore.store.redis.RedisStore.proxy>` places :code:`x` into the Redis server and returns a proxy that will resolve to :code:`x`.
The proxy can be cheaply serialized and sent anywhere that can still access the Redis server and be able to correctly resolve itself.

See :doc:`Advanced Usage <./advanced>` for more detailed functionality in ProxyStore.

Examples
--------

Examples of integrating ProxyStore into distributed applications built on `FuncX <https://funcx.org/>`_ and `Parsl <https://parsl-project.org/>`_ are `here <https://github.com/gpauloski/ProxyStore/tree/main/examples>`_.
