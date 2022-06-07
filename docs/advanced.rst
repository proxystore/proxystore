Advanced
########

Stores
======

Asynchronous Resolving
----------------------

It is common in distributed computation for inputs to functions executed
remotely to not be needed immediately upon execution.
Store implementations
(E.g., :any:`RedisStore <proxystore.store.redis.RedisStore>`) provide support
for asynchronously resolving proxies to overlap communication and computation.

.. code-block:: python

   import proxystore as ps

   def complex_function(large_proxied_input):
       ps.proxy.resolve_async(large_proxied_input)

       # More computation...

       # First access to the proxy will not be as expensive because
       # of the asynchronous resolution
       compute_input(large_proxied_input)

Caching
-------

:any:`Stores <proxystore.store.base.Store>` provides built in caching
functionality.
Caches are local to the Python process but will speed up the resolution when
multiple proxies refer to the same object.

Transactional Guarantees
------------------------

ProxyStore is designed around optimizing the communication of ephemeral data
(e.g., inputs and outputs of functions) which is typically write-once,
read-many. Thus, ProxyStore does not provides any guarantees about object
versions if a user manually overwrites an object.

Proxies
=======

Proxies can be created easily using ProxyStore.

.. code-block:: python

   import proxystore as ps

   def resolve_object(...):
       # Function that produces the object of interest
       return obj

   p = ps.proxy.Proxy(ps.factory.LambdaFactory(resolve_object))

:code:`resolve_object()` will be called when the proxy :code:`p` does its
just-in-time resolution, and then :code:`p` will behave exactly like
:code:`obj`.
The :any:`LambdaFactory <proxystore.factory.LambdaFactory>` accepts any
callable Python object (functions, lambdas, etc.).

Proxies are powerful because they can intercept and redefine functionality of
an object while emulating the rest of the objects behavior.

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

The ProxyStore :any:`Proxy <proxystore.proxy.Proxy>` is built on the proxy
from `lazy-object-proxy <https://github.com/ionelmc/python-lazy-object-proxy>`_
which intercepts all calls to the object's magic functions
(:code:`__func_name__()` functions) and forwards the calls to the wrapped
object. If the wrapped object has not been resolved yet, the proxy calls the
:any:`Factory <proxystore.factory.Factory>` that was passed to the proxy
constructor to retrieve the object that should be wrapped.

Generally, a proxy is only ever resolved once.
However, when a proxy is serialized, only the factory is serialized, and when
the proxy is deserialized again and used, the factory will be called again to
resolve the object.

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
