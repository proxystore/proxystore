A [`Store`][proxystore.store.base.Store] is initialized with a
[`Connector`][proxystore.connectors.protocols.Connector] instance and provides
extra functionality. Similar to the
[`Connector`][proxystore.connectors.protocols.Connector], the
[`Store`][proxystore.store.base.Store] exposes `evict`, `exist`, `get`, and `put`
operations; however, these operations act on Python objects rather than
[`bytes`][bytes]. The [`Store`][proxystore.store.base.Store] will (de)serialize
objects accordingly before invoking the corresponding operation on the
[`Connector`][proxystore.connectors.protocols.Connector].
The [`Store`][proxystore.store.base.Store] also provides caching of operations
to reduce communication costs, and objects are cached after deserialization to
avoid duplicate deserialization.

However, instead of the application directly invoking these aforementioned
operations, the proxy method, also provided by the
[`Store`][proxystore.store.base.Store], is used. Calling
[`Store.proxy()`][proxystore.store.base.Store.proxy] puts an object in the
mediated channel and returns a proxy (see example below). The object is
serialized before being put in the mediated channel, a factory with the key
returned by the [`Connector`][proxystore.connectors.protocols.Connector] and
other information necessary to retrieve the object from the mediated channel
is generated, and then a new proxy, internalized with the factory, is returned.

```python title="Base Store Usage" linenums="1"
from proxystore.connectors.redis import RedisConnector
from proxystore.proxy import Proxy
from proxystore.store import Store
from proxystore.store import register_store

def my_function(x: MyDataType) -> ...:
    assert isinstance(x, MyDataType)  # (1)!
    # More computation...

store = Store('my-store', RedisConnector(...)) # (2)!
register_store(store)  # (3)!

my_object = MyDataType(...) # (4)!
p = store.proxy(my_object)
isinstance(p, Proxy)

my_function(p) # (5)!
```

1. `x` is resolved from "my-store" on the first use of `x`.
2. The `Connector` defines the low-level communication method used by the `Store`.
3. Registering `store` globally enables proxies to reuse the same instance
   to improve performance.
4. Store the object and get a proxy.
5. Always succeeds regardless of if `p` is the true object or a proxy.

## Asynchronous Resolving

It is common in distributed computation for inputs to functions executed
remotely to not be needed immediately upon execution.
Proxies created by a [`Store`][proxystore.store.base.Store] support
asynchronous resolution to overlap communication and computation.

```python linenums="1"
from proxystore.store.utils import resolve_async

def complex_function(large_proxied_input):
   resolve_async(large_proxied_input)

   # More computation...

   # First access to the proxy will not be as expensive because
   # of the asynchronous resolution
   compute_input(large_proxied_input)
```

## Caching

The [`Store`][proxystore.store.base.Store] provides built in caching functionality.
Caches are local to the Python process but will speed up the resolution when
multiple proxies refer to the same object.

```python linenums="1"
from proxystore.store import Store

# Cache size of 16 is the default
Store('mystore', connector=..., cache_size=16)
```

## Transactional Guarantees

ProxyStore is designed around optimizing the communication of ephemeral data
(e.g., inputs and outputs of functions) which is typically write-once,
read-many. Thus, ProxyStore does not provide `update` semantics on keys.

## Serialization

All [`Store`][proxystore.store.base.Store] operation uses ProxyStore's provided
serialization utilities ([`proxystore.serialize`][proxystore.serialize]) by default.
However, the [`Store`][proxystore.store.base.Store] can be initialized with
custom default serializers or deserializers of the form:

```python linenums="1"
serializer = Callable[[Any], bytes]
deserializer = Callable[[bytes], Any]
```
Most methods also support specifying an alternative serializer or deserializer to the default.

In some cases, data may already be serialized in which case an identity
function can be passed as the serializer/deserializer (e.g., `#!python lambda x: x`).
Implementing a custom serializer may be beneficial for complex structures
where pickle/cloudpickle (the default serializers used by ProxyStore) are
innefficient. E.g.,

```python linenums="1"
import torch
import io

from proxystore.serialize import serialize
from proxystore.store import Store

def serialize_torch_model(obj: Any) -> bytes:
   if isinstance(obj, torch.nn.Module):
       buffer = io.BytesIO()
       torch.save(model, buffer)
       return buffer.read()
   else:
       # Fallback for unsupported types
       return serialize(obj)

mymodel = torch.nn.Module()

store = Store(...)
key = store.put(mymodel, serializer=serialize_torch_model)
```

Rather than providing a custom serializer or deserializer to each method
invocation, a default serializer and deserializer can be provided when
initializing a new [`Store`][proxystore.store.base.Store].
See Issue [#146](https://github.com/proxystore/proxystore/issues/146){target=_blank}
for further discussion on where custom serializers can be helpful.
