Advanced
########

Proxies
=======

Proxies are powerful because they can intercept and redefine functionality of an object while emulating the rest of the objects behavior.

.. code-block:: python

   import numpy as np
   import proxystore as ps

   x = np.array([1, 2, 3])

   class MyFactory(ps.factory.Factory):
       def __init__(self, obj):
           self.obj = obj

       def resolve(self):
           return self.obj

   p = ps.proxy.Proxy(MyFactory(x))

   # A proxy is an instance of its wrapped object
   assert isinstance(p, ps.proxy.Proxy)
   assert isinstance(p, np.ndarray)

   # The proxy can do everything the numpy array can
   assert np.array_equal(p, [1, 2, 3])
   assert np.sum(p) == 6
   y = x + p
   assert np.array_equal(y, [2, 4, 6])

The ProxyStore :any:`Proxy <proxystore.proxy.Proxy>` is built on the proxy from `lazy-object-proxy <https://github.com/ionelmc/python-lazy-object-proxy>`_ and intercepts all calls to the object's magic functions (:code:`__func_name__()` functions) and forwards the calls to the wrapped object.
If the wrapped object has not been resolved yet, the proxy calls the :any:`Factory <proxystore.factory.Factory>` that was passed to the proxy constructor to retrieve the object that should be wrapped.

Generally, a proxy is only ever resolved once.
However, when a proxy is serialized, only the factory is serialized, and when the proxy is deserialized again and used, the factory will be called again to resolve the object.

Proxystore provides some useful utility functions for dealing with proxies.

.. code-block:: python

   import proxystore as ps

   p = ps.proxy.Proxy(...)

   # Check if a proxy has been resolved yet
   ps.proxy.is_resolved(p)

   # Force a proxy to resolve itself
   ps.proxy.resolve(p)

   # Extract the wrapped object from the proxy
   x = ps.proxy.extract(p)
   assert not isinstance(x, ps.proxy.Proxy)

   # Begin resolving a Factory asynchronously.
   # Note: only supported by Factories the implement resolve_async()
   ps.proxy.resolve_async(p)

Stores
======

Proxies are valuable in distributed computing for delaying object communication of objects.
In this context, passing a proxy is like passing a reference that also knows how to dereference itself as soon as it is needed.

ProxyStore provides a :any:`Store <proxystore.store.base.Store>` interface for passing objects via proxies that resolve themselves from object stores.
E.g., if you have some distributed object store accesible by all devices in your environment, you can create a proxy that will resolve itself to an object that lives in the store.

ProxyStore provides many :any:`Store <proxystore.store.base.Store>` implementations:
   
  * :any:`LocalStore <proxystore.store.local.LocalStore>`: Stores objects in local process memory.
  * :any:`RedisStore <proxystore.store.redis.RedisStore>`: Uses a Redis server for storing objects.
  * :any:`FileStore <proxystore.store.file.FileStore>`: Uses a globally accessible file system directory for storing objects.
  * :any:`GlobusStore <proxystore.store.globus.GlobusStore>`: Used to transfer objects between two Globus endpoints.

Redis Example
-------------

.. code-block:: python

   import proxystore as ps

   store = ps.store.init_store(
       ps.store.STORES.REDIS, name='redis', hostname=REDIS_HOST, port=REDIS_PORT
   )

   # An already initialized store can be retrieved
   store = ps.store.get_store('redis')

   # Stores have basic get/set functionality
   key = store.set(my_object)
   assert my_object == store.get(key)

   # Place an object in the store and return a proxy
   p = store.proxy(my_other_object)

   # Get a proxy reference for an object already in the store
   p = store.proxy(key=key)

The provided store implementations also provide factories that know how to interact with the store and initialize the store interface if needed again.
For example, if a :any:`RedisStore <proxystore.store.redis.RedisStore>` is initialized in one Python process and a proxy referencing an object in the Redis server is created, serialized, and sent to another Python process, the proxy will be able to initialize another :any:`RedisStore <proxystore.store.redis.RedisStore>` interface on the new process to resolve the object.

Asynchronous Resolving
----------------------

It is common in distributed computation for inputs to functions executed remotely to not be needed immediately upon execution.
Store implementations such as :any:`RedisStore <proxystore.store.redis.RedisStore>` provide support for asynchronously resolving proxies to overlap communication and computation.

.. code-block:: python

   import proxystore as ps

   def complex_function(large_proxied_input):
       ps.proxy.resolve_async(large_proxied_input)
       
       # More computation...

       # First access to the proxy will not be as expensive because
       # of the asynchronous resolution
       compute_input(large_proxied_input)

The method by which factories asynchronously resolve objects is unique to the factory.
For example, :any:`RedisFactory <proxystore.store.redis.RedisFactory>` will spawn a new thread to communicate with the remote Redis server to retrieve the object.
A future for the thread is store inside the factory (and therefore inside the proxy).

Caching
-------

The :any:`RemoteStore <proxystore.store.remote.RemoteStore>` provides built in caching functionality for custom Store implementations such as :any:`RedisStore <proxystore.store.redis.RedisStore>`.
Caches are local to the Python process but will speed up the resolution when multiple proxies refer to the same object in the Redis server.

Transactional Guarentees
------------------------

By default, ProxyStore does not guarentee a proxy resolves with the most recent version of an object.
If the object associated with `custom-key` in the backend store later changes before the proxy has been resolved, it is not guarenteed which version of the object will be returned (generally because the older version may be cached locally).
:any:`Store.proxy() <proxystore.store.base.Store.proxy>` accepts a :code:`strict` flag to enforce that the proxy will always resolve to the most up to date version of the object associated with `custom-key`.

Note that not all :any:`Store <proxystore.store.base.Store>` types support mutable objects so :code:`strict` may be unused.

Known Issues
------------

No known issues currently.
