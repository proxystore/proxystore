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
from proxystore.connectors.local import LocalConnector
from proxystore.proxy import Proxy
from proxystore.store import Store

def process(x: dict[str, str]) -> None:
    assert isinstance(x, dict)  # (1)!
    assert x['hello'] == 'world'
    # More computation using x...

with Store(
    'example',
    connector=LocalConnector(),  # (2)!
    register=True,  # (3)!
) as store:
    x = {'hello': 'world'}
    proxy = store.proxy(x)  # (4)!
    assert isinstance(proxy, Proxy)
    process(proxy)  # (5)!
```

1. `x` is resolved from the store named "example" on the first use of `x` and `x` then acts as a transparent reference to the target object (the dictionary).
2. The `Connector` defines the low-level communication method used by the `Store`. Here, the [`LocalConnector`][proxystore.connectors.local.LocalConnector] stores data in the processes memory, but other connectors are provided to remote storage and transfer services.
3. Passing the `register=True` will call [`register_store()`][proxystore.store.register_store] automatically to register the instance globally by name.
   This enables proxies to reuse the same store instance to improve performance.
4. Store the object and get a proxy.
5. Always succeeds regardless of if `proxy` is the true object or a proxy.

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
Store('example', connector=..., cache_size=16)
```

## Transactional Guarantees

ProxyStore is designed around optimizing the communication of ephemeral data
(e.g., inputs and outputs of functions) which is typically write-once,
read-many. Thus, ProxyStore does not provide `update` semantics on keys.

## Serialization

All [`Store`][proxystore.store.base.Store] operations use ProxyStore's provided
serialization utilities ([`proxystore.serialize`][proxystore.serialize]) by default.
However, the [`Store`][proxystore.store.base.Store] can be initialized with
custom default serializers or deserializers of the form:

```python linenums="1"
serializer = Callable[[Any], BytesLike]
deserializer = Callable[[BytesLike], Any]
```
Implementing a custom serializer may be beneficial for complex structures
where pickle/cloudpickle (the default serializers used by ProxyStore) are
innefficient. E.g.,

```python linenums="1"
import torch
import io

from proxystore.serialize import serialize, deserialize, SerializationError
from proxystore.store import Store

def serialize_torch_model(obj: Any) -> bytes:
    if isinstance(obj, torch.nn.Module):
        buffer = io.BytesIO()
        buffer.write(b'PT\n')
        torch.save(obj, buffer)
        return buffer.getvalue()
    else:
        # Fallback for unsupported types
        return serialize(obj)

def deserialize_torch_model(raw: BytesLike) -> Any:
    try:
        return deserialize(raw)
    except SerializationError:
        buffer = io.BytesIO(raw)
        assert buffer.readline() == b'PT\n'
        return torch.load(buffer, weights_only=False)

model = torch.nn.Module()

with Store(
    ...,
    serializer=serialize_torch_model,
    deserializer=deserialize_torch_model,
) as store:
    key = store.put(model)
    model = store.get(key)
```

!!! tip

    In some cases, data may already be serialized in which case an identity function can be passed as the serializer (e.g., `#!python lambda x: x`).
    However, `populate_target=False` should also be set in this case to avoid prepopulating the proxy with the serialized target object.
    See the [`Store`][proxystore.store.base.Store] docstring for more information.

Alternative, custom serializers and deserializers can be passed to [`Store`][proxystore.store.base.Store], overriding the class defaults.
See Issue [#146](https://github.com/proxystore/proxystore/issues/146){target=_blank}
for further discussion on where custom serializers can be helpful.
