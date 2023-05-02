Proxies are commonly used to add additional functionality to their
*target object* or enforce assertions prior to forwarding operations to the
target. For example, a proxy can wrap sensitive objects with access control or
provide caching for expensive operations.

Two valuable properties that a proxy can provide are *transparency* and
*lazy resolution*. A transparent proxy behaves identically to its target object
by forwarding all operations on itself to the target. For example, given a
proxy `p` of an arbitrary object `v`, the types of `v` and `p` will be
equivalent, i.e., `#!python isinstance(p, type(v))` and any operation on `p`
will invoke the corresponding operation on `v`.

A lazy or virtual proxy provides *just-in-time* resolution of its
target object. In this case, the proxy is initialized with a *factory*
rather than the target object. A factory is any object that is callable
like a function and returns the target object. The proxy is lazy in
that it does not call the factory to retrieve the target until it is first
accessed. This process is referred to as resolving the proxy. Functionally,
proxies have both pass-by-reference and pass-by-value attributes. The eventual
user of the proxied data gets a copy, but unnecessary copies are avoided when
the proxy is passed between multiple functions.

## Creating Proxies

```python linenums="1"
from proxystore.proxy import Proxy

def resolve_object(...):
   # Function that produces the object of interest
   return obj

p = Proxy(resolve_object)
```

`#!python resolve_object()` will be called when the proxy `p` does its
just-in-time resolution, and then `p` will behave exactly like `obj`.
A factory for a [`Proxy`][proxystore.proxy.Proxy] can be
any callable object (i.e., object which implements `__call__`).

Proxies are powerful because they can intercept and redefine functionality of
an object while emulating the rest of the objects behavior.

```python linenums="1" hl_lines="15 16 18 19 20 21"
import numpy as np
from proxystore.proxy import Proxy

x = np.array([1, 2, 3])

class MyFactory():
   def __init__(self, obj):
       self.obj = obj

   def __class__(self):
       return self.obj

p = Proxy(MyFactory(x))

assert isinstance(p, Proxy) # (1)!
assert isinstance(p, np.ndarray)

assert np.array_equal(p, [1, 2, 3]) # (2)!
assert np.sum(p) == 6
y = x + p
assert np.array_equal(y, [2, 4, 6])
```

1. A proxy is an instance of its wrapped object.
2. The proxy can do everything the numpy array can.

The ProxyStore [`Proxy`][proxystore.proxy.Proxy] is built on the proxy from
[`lazy-object-proxy`](https://github.com/ionelmc/python-lazy-object-proxy){target=_blank}
which intercepts all calls to the object's magic functions
(`#!python __func_name__()` functions) and forwards the calls to the
factory that was passed to the proxy constructor to retrieve the object that
should be wrapped.

Generally, a proxy is only ever resolved once. However, when a proxy is
serialized, only the factory is serialized, and when the proxy is deserialized
again and used, the factory will be called again to resolve the object.

## Interacting with Proxies

While a proxy can be used just as one would normally use the proxy's target
object, additional functions are provided for interacting with the proxy
directly.

```python
from proxystore import proxy

p = proxy.Proxy(...)

# Check if a proxy has been resolved yet
proxy.is_resolved(p)

# Force a proxy to resolve itself
proxy.resolve(p)

# Extract the wrapped object from the proxy
x = proxy.extract(p)
assert not isinstance(x, proxy.Proxy)
```
