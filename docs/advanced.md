# Advanced

## Proxies

Proxies can be created easily using ProxyStore.

```python
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

```python
import numpy as np
from proxystore.proxy import Proxy

x = np.array([1, 2, 3])

class MyFactory():
   def __init__(self, obj):
       self.obj = obj

   def __class__(self):
       return self.obj

p = Proxy(MyFactory(x))

# A proxy is an instance of its wrapped object
assert isinstance(p, Proxy)
assert isinstance(p, np.ndarray)

# The proxy can do everything the numpy array can
assert np.array_equal(p, [1, 2, 3])
assert np.sum(p) == 6
y = x + p
assert np.array_equal(y, [2, 4, 6])
```

The ProxyStore [`Proxy`][proxystore.proxy.Proxy] is built on the proxy
from [`lazy-object-proxy`](https://github.com/ionelmc/python-lazy-object-proxy)
which intercepts all calls to the object's magic functions
(`#!python __func_name__()` functions) and forwards the calls to the wrapped
object. If the wrapped object has not been resolved yet, the proxy calls the
factory that was passed to the proxy constructor to retrieve the object that
should be wrapped.

Generally, a proxy is only ever resolved once.
However, when a proxy is serialized, only the factory is serialized, and when
the proxy is deserialized again and used, the factory will be called again to
resolve the object.

### Utilities

Proxystore provides some useful utility functions for dealing with proxies.

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

### Other Uses

Proxies can be used add functionality to existing objects.
Two common examples are access control and partial resoluion.

## Stores

### Asynchronous Resolving

It is common in distributed computation for inputs to functions executed
remotely to not be needed immediately upon execution.
Proxies created by a [`Store`][proxystore.store.base.Store] support
asynchronous resolution to overlap communication and computation.

```python
from proxystore.store.utils import resolve_async

def complex_function(large_proxied_input):
   resolve_async(large_proxied_input)

   # More computation...

   # First access to the proxy will not be as expensive because
   # of the asynchronous resolution
   compute_input(large_proxied_input)
```

### Caching

[`Store`][proxystore.store.base.Store] provides built in caching functionality.
Caches are local to the Python process but will speed up the resolution when
multiple proxies refer to the same object.

```python
from proxystore.store.file import FileStore

# Cache size of 16 is the default
FileStore('mystore', store_dir='/tmp/proxystore', cache_size=16)
```

### Transactional Guarantees

ProxyStore is designed around optimizing the communication of ephemeral data
(e.g., inputs and outputs of functions) which is typically write-once,
read-many. Thus, ProxyStore does not provides any guarantees about object
versions if a user manually overwrites an object.

### Serialization

All [`Store`][proxystore.store.base.Store] operation uses ProxyStore's provided
serialization utilities ([proxystore.serialize][]) by default. However,
all [`Store`][proxystore.store.base.Store] methods that move data in or out of
the store can be provided custom serializers or deserializers of the form:

```python
serializer = Callable[[Any], bytes]
deserializer = Callable[[bytes], Any]
```

In some cases, data may already be serialized in which case an identity
function can be passed as the serializer/deserializer (e.g., `#!python lambda x: x`).
Implementing a custom serializer may be beneficial for complex structures
where pickle/cloudpickle (the default serializers used by ProxyStore) are
innefficient. E.g.,

```python
import torch
import io

from proxystore.serialize import serialize
from proxystore.store.redis import RedisStore

def serialize_torch_model(obj: Any) -> bytes:
   if isinstance(obj, torch.nn.Module):
       buffer = io.BytesIO()
       torch.save(model, buffer)
       return buffer.read()
   else:
       # Fallback for unsupported types
       return serialize(obj)

mymodel = torch.nn.Module()

store = RedisStore(...)
key = store.set(mymodel, serializer=serialize_torch_model)
```

See Issue #146 for further discussion.
