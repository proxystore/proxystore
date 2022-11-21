.. _get-started:

Get Started
###########

.. figure:: static/overview.png
   :align: center
   :figwidth: 100 %
   :alt: ProxyStore Overview

   ProxyStore allows developers to communicate objects via *proxies*. Proxies
   act as lightweight references that resolve to a *target* object upon use.
   Communication via proxies gives applications the illusion that objects
   are moving through a specified path (e.g., through a network
   socket, cloud server, workflow engine, etc.) while the true path the data
   takes is different. Transporting the lightweight proxies through the
   application or systems can be far more efficient and reduce overheads.

Overview
--------

ProxyStore provides a unique interface to object stores through transparent
object proxies that is designed to simplify the use of object stores for
transferring large objects in distributed applications.

`Proxies` are used to intercept and redefine operations on a `target` object.
A `transparent` proxy behaves identically to its target object
because the proxy forwards all operations on itself to the target.
A `lazy` proxy provides just-in-time `resolution` of the target object via
a `factory` function. Factories return the target object when called, and a
proxy, initialized with a factory, will delay calling the factory to retrieve
the target object until the first time the proxy is accessed.

ProxyStore uses lazy transparent object proxies as the interface to object
stores. When an object is proxied, the object is placed in the specified
object store, a factory containing the information needed to retrieve the
object from the store is created, and a proxy, initialized with the factory,
is returned.
The resulting proxy is essentially a lightweight reference to the target that
will resolve itself to the target and behave as the target once the proxy
is first used.
Thus, proxies can be used anywhere in-place of the true object and will
resolve themselves without the program being aware.

ProxyStore provides the proxy interface to a number of commonly used object
stores as well as the :any:`Proxy <proxystore.proxy.Proxy>` and
:class:`~proxystore.factory.Factory` building blocks to allow developers
to create powerful just-in-time resolution functionality for Python objects.

Installation
------------

.. code-block:: bash

   $ pip install proxystore

Serving :doc:`ProxyStore Endpoints <./guides/endpoints>` requires installing
using the extras install.

.. code-block:: bash

   $ pip install proxystore[endpoints]

See :doc:`Contributing <./contributing>` if you are installing for local
development.

Usage
-----

ProxyStore is intended to be used via the
:class:`~proxystore.store.base.Store` interface which provide the
:py:meth:`~proxystore.store.base.Store.proxy` method for placing objects
in stores and creating proxies that will resolve to the associated object in
the store.

ProxyStore provides many :class:`~proxystore.store.base.Store`
implementations (list :py:mod:`here <proxystore.store>`) and more can be
added by extending the :class:`~proxystore.store.base.Store` class.

The following example uses the
:class:`~proxystore.store.redis.RedisStore` to interface with a
running Redis server using proxies.

.. code-block:: python

   from proxystore.store import get_store
   from proxystore.store import register_store
   from proxystore.store.redis import RedisStore

   store = RedisStore(name='my-store', hostname='localhost', port=1234)
   register_store(store)

   # A registered store can be retrieved by name
   store = get_store('my-store')

   # Stores have basic get/set functionality
   key = store.set(my_object)
   assert my_object == store.get(key)

   # Place an object in the store and return a proxy
   p = store.proxy(my_object)

   # The proxy, when used, will behave as the target
   assert isinstance(p, type(my_object))

This proxy, :code:`p`, can be cheaply serialized and communicated to any
arbitrary Python process as if it were the target object itself. Once the
proxy is used on the remote process, the underlying factory function will
be executed to retrieve the target object from the Redis server.

Using the :class:`~proxystore.store.base.Store` store interface allows
developers to write code without needing to worry about how data communication
is handled and reduces the number of lines of code that need to be changed
when adding or changing the communication methods.

For example, if you want to execute a function and the input data may be
passed directly, via a key to an object in Redis, or as a filepath to a
serialized object on disk, you will need boilerplate code that looks like:

.. code-block:: python

   def my_function(input: MyDataType | str | ...) -> None:
       if is_filepath(input_data):
           data = read_and_deserialize(input)
       elif is_redis_key(input_data):
           data = redis_client.get(input)
       elif is_other_communication_method(input_data):
           ...
       elif isinstance(input, MyDataType):
           data = input
       else:
            raise ValueError(...)

       # Compute using the data

This function is hard to type and must be extended every time a new
communication method is used. With proxies, all of the boilerplate code
can be removed because the proxy will contain within itself all of the
necessary code to resolve the object.

.. code-block:: python

   def my_function(input: MyDataType) -> None:
       # Always true even if input is a proxy
       assert isinstance(input, MyDataType)

       # Compute using the data

In this model, only the producer of the data needs to be aware of which
ProxyStore backend to use, and no modification to consumer code are ever
required.

**How is this more efficient?**

The ProxyStore model can improve application performance in many ways:

1. Unused proxies are not resolved so not resources/time were wasted on the
   communication.
2. Object communication always takes place between the producer, the store, and
   the consumer meaning communication is not wasted on intermediate processes
   which have a proxy but do not use it.
3. Different backends can be used that are optimized for specific usage
   patterns.
4. Proxies have built-in caching for frequently used objects
   (:ref:`advanced-caching`).

See :doc:`Advanced Usage <./advanced>` to learn more!

Related
-------

**Examples:**
Examples of integrating ProxyStore into distributed applications built on
`FuncX <https://funcx.org/>`_ and `Parsl <https://parsl-project.org/>`_ are
`here <https://github.com/proxystore/proxystore/tree/main/examples>`_.

**Benchmarks:**
ProxyStore benchmarks are maintained at `<https://github.com/proxystore/proxystore-benchmarks>`_.

Known Issues
------------

* :doc:`ProxyStore Endpoints <./guides/endpoints>` are not supported for
  Python 3.7 on ARM-based Macs because
  `aiortc <https://aiortc.readthedocs.io/>`_ does not have the corresponding
  wheels. The base ProxyStore package can still be installed on this
  software/hardware configurations---just not with the ``endpoints`` extras.
