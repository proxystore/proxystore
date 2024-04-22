# Frequently Asked Questions

*[Open a new issue](https://github.com/proxystore/proxystore/issues){target=_bank} if you have a question not answered in the FAQ, Guides, or API docs.*

## Working with Proxies

### How do I check the type of a proxy?

To check if an object is a proxy, use `#!python isinstance(obj, Proxy)`.
This will not resolve the proxy.

Checking the type of a proxy's target object requires more care because `#!python isinstance(proxy, MyType)` will resolve the proxy.
This can be avoided by doing a direct type comparisons (e.g., `#!python type(proxy) == MyType)`) but this will mean that type comparisons with subclasses will not work.
Otherwise, resolving a proxy when using [`isinstance()`][isinstance] is unavoidable.

```python linenums="1"
from proxystore.proxy import Proxy
from proxystore.proxy import is_resolved

proxy = Proxy(lambda: 42)
assert not is_resolved(proxy)

assert isinstance(proxy, Proxy)
assert not is_resolved(proxy)

assert type(proxy) != str
assert not is_resolved(proxy)

assert not isinstance(proxy, str)
assert is_resolved(proxy)
```

### What is resolving my proxy?

Certain data structures can unintenionally resolve a proxy.
This is because the [`Proxy`][proxystore.proxy.Proxy] type forwards *all* special methods to the target object.
For example, data structures which use the hash of an object, such as [`set()`][set] or [`dict()`][dict], will cause a proxy to resolve because `proxy.__hash__()` is forwarded to the target object's `__hash__()`.

```python linenums="1"
from proxystore.proxy import Proxy
from proxystore.proxy import is_resolved

proxy = Proxy(lambda: 42)
assert not is_resolved(proxy)

my_set = set()
my_set.add(proxy)
assert is_resolved(proxy)
```

There are a few mechanisms for determining when a proxy is getting resolved while debugging.

* Use [`is_resolved()`][proxystore.proxy.is_resolved].
* Use a fake factory which raises an error to halt the program and get a traceback at the point the proxy was resolved.
  ```python linenums="1"
  from proxystore.proxy import Proxy

  def alert_factory() -> None:
      raise RuntimeError('Proxy was resolved!')

  proxy = Proxy(alert_factory)
  ```
* Proxies created via a [`Store`][proxystore.store.base.Store] will produce `DEBUG` level logs when a proxy is created and resolved.
  Enabling `DEBUG` level logging can help determine when or how often a proxy is getting resolved.
  Look for the `PUT` and `GET` messages indicating a target object was put in the store when creating the proxy and when the target object is retrieved when the proxy is resolved, respectively.

### How to I serialize a proxy?

A [`Proxy`][proxystore.proxy.Proxy] can be serialized using most common serializers.

```python linenums="1"
from proxystore.proxy import Proxy

def factory() -> int:
    return 42

proxy = Proxy(factory)

# Cloudpickle
import cloudpickle
dump = cloudpickle.dumps(proxy)
new_proxy = cloudpickle.loads(dump)
assert isinstance(new_proxy, Proxy)
assert new_proxy == 42

# Dill
import dill
dump = dill.dumps(proxy)
new_proxy = dill.loads(dump)
assert isinstance(new_proxy, Proxy)
assert new_proxy == 42

# Pickle
import pickle
dump = pickle.dumps(proxy)
new_proxy = pickle.loads(dump)
assert isinstance(new_proxy, Proxy)
assert new_proxy == 42
```

Importantly, only the factory of the proxy will be serialized, not the target object.
This means that the factory must be serializable (lambda functions, for example, are not serializable with [pickle][pickle]), and the serialized size of the proxy is a function of the factory and not the target object.
This typically means that proxies can be very efficiently serialized.
For example, here the target object is a large 10,000 character string but the serialized proxy is less than 200 bytes.

```python linenums="1"
import sys
import pickle
from proxystore.proxy import Proxy

target = 'x' * 10000
assert sys.getsizeof(target) >= 10000

def factory() -> str:
    return target

proxy = Proxy(factory)

dump = pickle.dumps(proxy)
assert sys.getsizeof(dump) < 200
```

## Static Type Checking

### How do I annotate a proxy type?

The [`Proxy`][proxystore.proxy.Proxy] is a generic type on its target object, and mypy will infer the correct type of the target from the provided factory function.
For example, the [`extract()`][proxystore.proxy.extract] function is annotated as follows.

```python linenums="1"
from typing import TypeVar
from proxystore.proxy import Proxy

T = TypeVar('T')

def extract(proxy: Proxy[T]) -> T:
    ...

proxy = Proxy(lambda: 42)
reveal_type(proxy)  # Revealed type is Proxy[int]

target = extract(proxy)
reveal_type(target)  # Revealed type is int
```

In the event a proxy's type is ambiguous, an annotation can be provided directly.
For example, this is the case with [`Store.proxy_from_key()`][proxystore.store.base.Store.proxy_from_key] because the type system cannot infer the target type from a key which is a tuple of metadata.

```python linenums="1"
from proxystore.connectors.local import LocalConnector
from proxystore.store import Store

with Store('example', LocalConnector()) as store:
    key = store.put('value')
    proxy: Proxy[str] = store.proxy_from_key(key)
```

### Can mypy infer attributes of a proxied type?

In general, no.
Attributes and methods of an object of type `T`, accessed indirectly via a [`Proxy[T]`][proxystore.proxy.Proxy], are ambiguous to mypy and will typically resolve to [`Any`][typing.Any].

However, ProxyStore provides a mypy plugin that, when enabled, will help mypy resolve types related to proxies correctly.
Check out the [mypy plugin][proxystore.mypy_plugin] to learn more.

## ProxyStore Endpoints

### Why are my endpoints not working?

Check out the [Endpoints Debugging Guide](guides/endpoints-debugging.md).
