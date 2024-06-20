# Frequently Asked Questions

*[Open a new issue](https://github.com/proxystore/proxystore/issues){target=_bank} if you have a question not answered in the FAQ, Guides, or API docs.*

## Working with Proxies

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

!!! tip

    The `#!python cache_defaults=True` and `target` flags can be used inside the [`Proxy`][proxystore.proxy.Proxy] constructor to cache the `__hash__` value of the target which will make the proxy hashable without needing to be resolved first.
    This only applies to hashable target objects.
    Similarly, passing `#!python populate_target=True` to [`Store.proxy()`][proxystore.store.base.Store.proxy] will automatically set these flags on the returned proxy.

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

### How do I serialize a proxy?

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

## Runtime Type Checking

### How do I check the type of a proxy?

To check if an object is a proxy, use `#!python isinstance(obj, Proxy)`.
This will not resolve the proxy.

Checking the type of a proxy's target object requires more care because `#!python isinstance(proxy, MyType)` will resolve the proxy.
This can be avoided by doing a direct type comparisons (e.g., `#!python type(proxy) == MyType)`) but this will mean that type comparisons with subclasses will not work.

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

If the target object is known when creating a proxy, the `cache_defaults` and `target` parameters can be used to cache the type of the target so that [`isinstance`][isinstance] checks do not need to resolve the proxy.

```python linenums="1"
from proxystore.proxy import Proxy
from proxystore.proxy import is_resolved

value = 'value'
proxy = Proxy(lambda: value, cache_defaults=True, target=value)
del proxy.__proxy_wrapped__  # (1)!
assert not is_resolved(proxy)

assert isinstance(proxy, str)  # (2)!
assert not is_resolved(proxy)
```

1. Passing `target=value` will create a proxy that is already resolved.
   Deleting the wrapped target object will "unresolve" the proxy.
2. [`isinstance`][isinstance] can be used safely because the `__class__` value of the target was cached inside the proxy instance.

### Why does `isinstance()` behave differently with proxies?

Generally, [`isinstance()`][isinstance] works the same with proxies as with other types.
However, there are some edge cases where behavior is different.
This is most common with special generic alias types such as [`typing.Mapping`][typing.Mapping] or [`typing.Sequence`][typing.Sequence].
Consider the following example, where a [`Proxy`][proxystore.proxy.Proxy] with a [`dict`][dict] target object is an instance of [`dict`][dict] but not [`typing.Mapping`][typing.Mapping].

```python linenums="1"
import collections
import typing
from proxystore.proxy import Proxy

my_dict = {}
assert isinstance(my_dict, dict)
assert isinstance(my_dict, typing.Mapping)
assert isinstance(my_dict, collections.abc.Mapping)

my_dict_proxy = Proxy(lambda: my_dict)
assert isinstance(my_dict_proxy, dict)
assert not isinstance(my_dict_proxy, typing.Mapping)
assert isinstance(my_dict_proxy, collections.abc.Mapping)
```

Here, the [`isinstance()`][isinstance] check fails with the aliased type [`typing.Mapping`][typing.Mapping] but succeeds with the ABC [`collections.abc.Mapping`][collections.abc.Mapping].
If you encounter a similar issue, try replacing the deprecated [`typing`][typing] aliases with the types defined in [`collections.abc`][collections.abc].

An alternative solution is to [`extract()`][proxystore.proxy.extract] the proxy before type checking.
This will incur the cost of resolving the proxy, if it was not already resolved, but [`isinstance()`][isinstance] checks will often resolve a proxy anyways.
See the related discussion on [checking the type of a proxy](#how-do-i-check-the-type-of-a-proxy).

```python linenums="1"
import typing
from proxystore.proxy import extract, Proxy

my_dict = Proxy(lambda: {})

if isinstance(my_dict, Proxy):
    my_dict = extract(my_dict)

assert isinstance(my_dict, typing.Mapping)
```

The `#!python isinstance(my_dict, Proxy)` check is not necessary in this specific example as we know `my_dict` is a [`Proxy`][proxystore.proxy.Proxy] instance.
However, this pattern is useful in the general case where you may have a type `T` or a [`Proxy[T]`][proxystore.proxy.Proxy].

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
