Advanced
########

Proxying
--------

ProxyStore provides the :any:`to_proxy() <proxystore.to_proxy()>` function to streamline the process of proxying an object in a method compatible with the backend.
However, an object can by manually proxied.
E.g.,

.. code-block:: python

   import numpy as np
   import proxystore as ps

   x = np.array([1, 2, 3])

   ps.init_redis_backend(hostname=REDIS_HOST, port=REDIS_PORT)
   # The RedisFactory constructor will place x into the store. If x was
   # manually placed into the store, passing x to the factory can be skipped.
   f = ps.factory.RedisFactory(
           x, key='my key', hostname=REDIS_HOST, port=REDIS_PORT)
   p = ps.proxy.Proxy(f)

The three steps (putting the object in the store, creating a factory, and making the proxy) are neatly handled by :any:`to_proxy() <proxystore.to_proxy()>`.

Note here that calling `f`, i.e., :code:`f()`, will return `x` from Redis.
`p` will automatically call `f` the first time `p` is used.

Custom backends and factories can be created and proxied following these steps.
All factories should inherit from :any:`proxystore.factory.Factory`, and all backend stores should inherit from :any:`proxystore.backend.store.Store`.

The :any:`proxystore.utils` modules provides many useful functions for interacting with proxies, including: :any:`is_resolved(p) <proxystore.utils.is_resolved()>`, :any:`extract(p) <proxystore.utils.extract()>`, and :any:`evict(p) <proxystore.utils.evict()>`.

Asynchronous Resolving
----------------------

A common design pattern for ProxyStore is a distributed computation environment where a coordinating node dispatches work (a input, function pair) to worker nodes.
Inputs can be passed from the coordinating node to worker nodes efficiently with proxies (and vice-versa with outputs).

Not all inputs are needed at the start of a function, so proxies can be asynchronously resolved allowing for an overlap of communication with the backend store and computation.

.. code-block:: python

   import proxystore as ps

   def complex_function(large_input):
       ps.utils.resolve_async(large_input)
       # more computation...
       compute_input(large_input)

Here, by calling :any:`resolve_async(proxy) <proxystore.utils.resolve_async()>` prior to the proxy being needed, the cost of communication with the backend store can be amortized.
The method by which factories asynchronously resolve objects is unique to the factory.
For example, :any:`RedisFactory <proxystore.factory.RedisFactory>` will spawn a new process to get the object from Redis and stores a future to the result in the factory.

Caching
-------

Following with the distributed design pattern from the previous section, it is common for a worker to execute many tasks that use the same input data.
Many of the ProxyStore backends, such as the Redis backend, will cache recently used value locally, speeding up the time it takes to initially resolve a proxy.

The number of cached key value pairs can be specified in the environment, e.g., :code:`export PROXYSTORE_CACHE_SIZE=16`, or passed as the :code:`cache_size` argument to the backend store constructor in manually initializing the backend.
If the cache size is 0, caching will not be used.

Transactional Guarentees
------------------------

By default, ProxyStore does not guarentee a proxy resolves with the most recent version of an object.
For example, let :code:`p = to_proxy(obj, key='custom-key')`.
If the object associated with `custom-key` in the backend store later changes before `p` has been resolved, it is not guarenteed which version of the object will be returned (generally because the older version may be cached locally).
To force strict guarentees that a proxy always resolves to the most recent value associated with a key, :code:`strict=True` can be passed to :any:`to_proxy() <proxystore.to_proxy()>`.

Known Issues
------------

No known issues currently.
