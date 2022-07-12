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
:any:`Factory <proxystore.factory.Factory>` building blocks to allow developers
to create powerful just-in-time resolution functionality for Python objects.

Installation
------------

.. code-block:: bash

   $ pip install proxystore

See :doc:`Contributing <./contributing>` if you are installing for local
development.

Usage
-----

ProxyStore is intended to be used via the
:any:`Store <proxystore.store.base.Store>` interface which provide the
:any:`proxy() <proxystore.store.base.Store.proxy>` method for placing objects
in stores and creating proxies that will resolve to the associated object in
the store.

ProxyStore provides many :any:`Store <proxystore.store.base.Store>`
implementations and more can be added by extending the
:any:`Store <proxystore.store.base.Store>` class.

.. list-table::
   :widths: 15 50
   :header-rows: 1
   :align: center

   * - Type
     - Use Case
   * - :any:`LocalStore <proxystore.store.local.LocalStore>`
     - In-memory object store local to the process. Useful for development.
   * - :any:`RedisStore <proxystore.store.redis.RedisStore>`
     - Store objects in a preconfigured Redis server.
   * - :any:`FileStore <proxystore.store.file.FileStore>`
     - Use a globally accessible file system for storing objects.
   * - :any:`GlobusStore <proxystore.store.globus.GlobusStore>`
     - Transfer objects between two Globus endpoints.
   * - :any:`EndpointStore <proxystore.store.endpoint.EndpointStore>`
     - [*Experimental*] P2P object stores for multi-site applications.

The following example uses the
:any:`RedisStore <proxystore.store.redis.RedisStore>` to interface with a
running Redis server using proxies.

.. code-block:: python

   import proxystore as ps

   store = ps.store.init_store(
       'redis', name='my-store', hostname=REDIS_HOST, port=REDIS_PORT
   )

   # An already initialized store can be retrieved
   store = ps.store.get_store('my-store')

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

Using the :any:`Store <proxystore.store.base.Store>` store interface allows
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

See :doc:`Advanced Usage <./advanced>` to learn more!

Examples
--------

Examples of integrating ProxyStore into distributed applications built on
`FuncX <https://funcx.org/>`_ and `Parsl <https://parsl-project.org/>`_ are
`here <https://github.com/gpauloski/ProxyStore/tree/main/examples>`_.
