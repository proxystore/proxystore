# Get Started

![ProxyStore Overview](static/proxystore-overview.svg){ width="75%" style="display: block; margin: 0 auto" }
> **Figure 1:** ProxyStore enables developers to communicate objects via *proxies*.
> Proxies act as lightweight references that resolve to a *target* object upon use.
> Communication via proxies gives applications the illusion that objects are
> moving through a specified path (e.g., through a network socket, cloud
> server, workflow engine, etc.) while the true path the data takes is
> different. Transporting the lightweight proxies through the application or
> systems can be more efficient---reduce overheads in the system.

## Overview

ProxyStore provides a unique interface to object stores, the transparent
object proxy, that is designed to simplify the use of object stores for
transferring large objects in distributed applications.

*Proxies* are used to intercept and redefine operations on a *target* object.
A *transparent* proxy behaves identically to its target object
because the proxy forwards all operations on itself to the target.
A *lazy* proxy provides just-in-time *resolution* of the target object via
a *factory* function. Factories return the target object when called, and a
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
stores as well as the [`Proxy`][proxystore.proxy.Proxy] and
[`Factory`][proxystore.factory.Factory] building blocks to allow developers
to create powerful just-in-time resolution functionality for Python objects.

## Usage

ProxyStore's main interface is the [`Store`][proxystore.store.base.Store] which provides the
[`Store.proxy()`][proxystore.store.base.Store.proxy] method for placing objects
in stores and creating proxies that will resolve to the associated object in
the store.

A [`Store`][proxystore.store.base.Store] is initialized with a
[`Connector`][proxystore.connectors.protocols.Connector] which serves as the
low-level interface to a byte-level object store.
ProxyStore provides many
[`Connector`][proxystore.connectors.protocols.Connector] implementations, and
third-party code can provide custom implementations provided they meet the
[`Connector`][proxystore.connectors.protocols.Connector] protocol
specification.

The following example uses the
[`RedisConnector`][proxystore.connectors.redis.RedisConnector] to interface
with an already running Redis server using proxies.

```python title="Basic ProxyStore Usage" linenums="1"
from proxystore.connectors.redis import RedisConnector
from proxystore.store import get_store
from proxystore.store import Store

store = Store(
    'my-store',
    RedisConnector(hostname='localhost', port=1234),
    register=True,
)

store = get_store('my-store')  # (1)!

key = store.put(my_object)  # (2)!
assert my_object == store.get(key)

p = store.proxy(my_object)  # (3)!

assert isinstance(p, type(my_object))  # (4)!
```

1. Passing `register=True` adds the store by name to a global registry, and registered store can be retrieved by name.
2. Stores have basic get/put functionality.
3. Create a proxy of an object, putting it into the store.
4. The proxy, when used, will behave as the target.

This proxy, `p`, can be cheaply serialized and communicated to any
arbitrary Python process as if it were the target object itself. Once the
proxy is used on the remote process, the underlying factory function will
be executed to retrieve the target object from the Redis server.

Using the [`Store`][proxystore.store.base.Store] interface allows
developers to write code without needing to worry about how data communication
is handled and reduces the number of lines of code that need to be changed
when adding or changing the communication methods.

For example, if you want to execute a function and the input data may be
passed directly, via a key to an object in Redis or as a filepath to a
serialized object on disk, you will need boilerplate code that looks like:

```python linenums="1"
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
```

This function is hard to type and must be extended every time a new
communication method is used. With proxies, all of the boilerplate code
can be removed because the proxy will contain within itself all of the
necessary code to resolve the object.

```python linenums="1"
def my_function(input: MyDataType) -> None:
   assert isinstance(input, MyDataType)  # (1)!

   # Compute using the data
```

1. Always true even if input is a proxy.

In this model, only the producer of the data needs to be aware of which
communication and storage system is being used, and no modification to the consumer code are required.

**How is this more efficient?**

The ProxyStore model can improve application performance in many ways:

1. Just-in-time resolution amortizes communication costs and avoids communication of unused objects.
2. Pass-by-reference reduces transfer overheads.
   Object communication always takes place between the producer, the store, and
   the consumer meaning the target object is not transferred through intermediate processes which have, but do not use, a proxy.
3. No external information is required to resolve a proxy so it is easy to use different communication channels that are optimized for specific usage patterns.
4. Proxies have built-in caching for frequently used objects.

## Learn More

See the [Concepts](concepts/index.md) to learn more about ProxyStore's core concepts.

ProxyStore provides many higher level interfaces for advanced application design patterns.

* [Distributed Futures](guides/proxy-futures.md)
* [Distributed Memory Management](guides/object-lifetimes.md)
* [Stream by Proxy](guides/streaming.md)
