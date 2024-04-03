# Object Lifetimes

*Last updated 20 March 2024*

The [`Store`][proxystore.store.base.Store], by default, leaves the responsibility of managing shared objects to the application.
For example, a object put into a [`Store`][proxystore.store.base.Store] will persist there until the key is manually evicted.
Some [`Connectors`][proxystore.connectors.protocols.Connector], and therefore [`Stores`][proxystore.store.base.Store], delete all of their objects when closed but this is not a specified requirement of the protocol.

ProxyStore, however, provides optional mechanisms for more automated management of shared objects.

<div class="center-table" markdown>

| Method | Supports keys? | Supports proxies? |
| :----- | :------------: | :--------------: |
| [Ephemeral Proxies](#ephemeral-proxies) | ✗ | ✓ |
| [Lifetimes](#lifetimes) | ✓ | ✓ |
| [Ownership](#ownership) | ✗ | ✓ |

</div>

*Note: Currently, these methods are mutually exclusive with each other.*

## Ephemeral Proxies

Setting the `evict=True` flag when creating a proxy of an object with
[`Store.proxy()`][proxystore.store.base.Store.proxy],
[`Store.proxy_batch()`][proxystore.store.base.Store.proxy_batch],
[`Store.proxy_from_key()`][proxystore.store.base.Store.proxy_from_key], or
[`Store.locked_proxy()`][proxystore.store.base.Store.locked_proxy]
marks the proxy as ephemeral (one-time use).
The factory will evict the object from the store when the proxy's factory is invoked for the first time to resolve the proxy.
This is useful when the a proxy will be created once and consumed once by an application.

A common side-effect of `evict=True` is obscure [`ProxyResolveMissingKeyError`][proxystore.store.exceptions.ProxyResolveMissingKeyError] tracebacks.
This commonly happens when a proxy is unintentionally resolved by another component of the program.
For example, certain serializers may attempt to inspect the proxy to optimize serialization but resolve the proxy in the process, or datastructures like [`set()`][set] access the `__hash__` method of the proxy which will resolve the proxy.
These accidental resolves will automatically evict the target object so later resolves of the proxy will fail.

If you run into these errors, try:

* Enabling `DEBUG` level logging to determine where unintentional proxy resolution is occurring.
  The [`Store`][proxystore.store.base.Store] will log every `GET` and `EVICT` operation on a key.
* Avoid use of datastructures or functions which unnecessarily resolve proxies.
* If avoiding use of the datastructures or functions causing the problem is not possible, consider using the `populate_target=True` flag when creating the proxy.
  The `populate_target` flag will return a proxy that is already resolved so the factory, which would evict the target object, does not need to be called until the proxy is serialized and then deserialized and resolved on a different process.

## Lifetimes

Shared objects in a [`Store`][proxystore.store.base.Store] can be associated with a [`Lifetime`][proxystore.store.lifetimes.Lifetime].
Lifetimes provide a management mechanism for keeping track of objects and cleaning them up when appropriate.

### Contextual Lifetime

The [`ContextLifetime`][proxystore.store.lifetimes.ContextLifetime] provides a simple interface for managing shared objects.
Objects added to a [`Store`][proxystore.store.base.Store] can be associated with the lifetime via the `lifetime` parameter supported by most [`Store`][proxystore.store.base.Store] methods.
Objects associated with the lifetime are evicted when the lifetime is closed/ended.

```python linenums="1" title="Contextual Lifetime"
from proxystore.store.base import Store
from proxystore.store.lifetimes import ContextLifetime

store = Store(...)

lifetime = ContextLifetime(store)  # (1)!

key = store.put('value', lifetime=lifetime)  # (2)!
proxy = store.proxy('value', lifetime=lifetime)  # (3)!

lifetime.close()  # (4)!
assert not store.exists(key)

store.close()  # (5)!
```

1. The [`ContextLifetime`][proxystore.store.lifetimes.ContextLifetime] and all its associated objects must be associated with the same [`Store`][proxystore.store.base.Store].
2. A new key can be automatically associated with a lifetime.
3. The target object of a proxy can be automatically associated with a lifetime.
4. Ending a lifetime will cause all of its associated objects to be evicted.
5. The [`Store`][proxystore.store.base.Store] should be closed after any associated lifetimes because lifetimes use the [`Store`][proxystore.store.base.Store] for cleanup.

The [`ContextLifetime`][proxystore.store.lifetimes.ContextLifetime] can be used as a context manager.

```python linenums="1" title="Contextual Lifetime"
from proxystore.store.base import Store
from proxystore.store.lifetimes import ContextLifetime

store = Store(...)

with ContextLifetime(store) as lifetime:
    key = store.put('value', lifetime=lifetime)
    proxy = store.proxy('value', lifetime=lifetime)

assert not store.exists(key)

store.close()  # (5)!
```

### Leased Lifetime

The [`LeaseLifetime`][proxystore.store.lifetimes.LeaseLifetime] provides time-based object lifetimes.
Each [`LeaseLifetime`][proxystore.store.lifetimes.LeaseLifetime] has an associated expiration time after which any associated objects will be evicted.
The lease can be extended as needed with [`extend()`][proxystore.store.lifetimes.LeaseLifetime.extend] or ended early [`close()`][proxystore.store.lifetimes.LeaseLifetime.close].

```python linenums="1" title="Leased Lifetime"

from proxystore.store.base import Store
from proxystore.store.lifetimes import LeaseLifetime

with Store(...) as store:
    lifetime = LeaseLifetime(store, expiry=10)  # (1)!

    key = store.put('value', lifetime=lifetime)
    proxy = store.proxy('value', lifetime=lifetime)

    lifetime.extend(5)  # (2)!

    time.sleep(20)  #(3)!

    assert lifetime.done()  #(4)!
    assert not store.exists(key)
```

1. Create a new lifetime with a current lease of ten seconds.
2. Extend the lease by another five seconds.
3. Sleep for longer than our current lease.
4. Lease has expired so the lifetime has ended and associated objects have been evicted.

### Static Lifetime

A static lifetime indicates that the associated objects should live for the remainder of the lifetime of the running process which created the object.
ProxyStore does not yet support a `StaticLifetime` class, but static lifetimes can be achieved with the use of a [`ContextLifetime`][proxystore.store.lifetimes.ContextLifetime] and an [atexit][atexit] handler.

```python linenums="1" title="Static Lifetime"
from proxystore.store.base import Store
from proxystore.store.lifetimes import ContextLifetime
from proxystore.store.lifetimes import register_lifetime_atexit

store = Store(...)

lifetime = ContextLifetime(store)  # (1)!
register_lifetime_atexit(lifetime, close_store=True)  # (2)!

key = store.put('value', lifetime=lifetime)  # (3)!
proxy = store.proxy('value', lifetime=lifetime)
```

1. Create and use a [`ContextLifetime`][proxystore.store.lifetimes.ContextLifetime] as normal.
2. Register an [atexit][atexit] handler with [`register_lifetime_atexit`][proxystore.store.lifetimes.register_lifetime_atexit].
   The `close_store` flag is `True` by default and will close the [`Store`][proxystore.store.base.Store] after the lifetime has been cleaned up.
3. Objects associated with the lifetime will be cleaned up when the program exits.

Additional tips:

1. The lifetime can be closed early if needed.
2. Closing the [`Store`][proxystore.store.base.Store] at the end of the program but before the [atexit][atexit] handler has executed can cause undefined behaviour.
   Let the handler perform all cleanup.
3. [atexit][atexit] does not guarantee that the handler will be called in some unexpected process shutdown cases.

## Ownership

An [`OwnedProxy`][proxystore.store.ref.OwnedProxy], created by [`Store.owned_proxy()`][proxystore.store.base.Store.owned_proxy], provides an alternative to the default [`Proxy`][proxystore.proxy.Proxy] which enforces Rust-like ownership and borrowing rules for objects in a [`Store`][proxystore.store.base.Store].

1. Each *target* object of type `T` in the global store has an associated [`OwnedProxy[T]`][proxystore.store.ref.OwnedProxy].
2. There can only be one [`OwnedProxy[T]`][proxystore.store.ref.OwnedProxy] for any *target* in the global store.
3. When an [`OwnedProxy[T]`][proxystore.store.ref.OwnedProxy] goes out of scope (e.g., gets garbage collected), the associated *target* is removed from the global store.

An [`OwnedProxy[T]`][proxystore.store.ref.OwnedProxy] can be borrowed without relinquishing ownership.
This requires two additional rules.

1. At any given time, you can have either one mutable reference to the *target*, a [`RefMutProxy[T]`][proxystore.store.ref.RefMutProxy], or any number of immutable references, a [`RefProxy[T]`][proxystore.store.ref.RefProxy].
2. References must always be valid. I.e., you cannot delete an [`OwnedProxy[T]`][proxystore.store.ref.OwnedProxy] while it has been borrowed via a [`RefProxy[T]`][proxystore.store.ref.RefProxy] or [`RefMutProxy[T]`][proxystore.store.ref.RefMutProxy].

Reference proxy types can be created and used using:
[`borrow()`][proxystore.store.ref.borrow],
[`mut_borrow()`][proxystore.store.ref.mut_borrow],
[`clone()`][proxystore.store.ref.clone],
[`into_owned()`][proxystore.store.ref.into_owned], and
[`update()`][proxystore.store.ref.update].

The [`submit()`][proxystore.store.scopes.submit] associates proxy references with the scope of a function executed by a function executor, such as a [`ProcessPoolExecutor`][concurrent.futures.ProcessPoolExecutor] or FaaS system.
This wrapper function ensures that immutable or mutable borrows of a value passed to a function are appropriately removed once the function completes.

```python linenums="1" title="Reference Lifetime Scopes"
from concurrent.futures import Future
from concurrent.futures import ProcessPoolExecutor
from proxystore.store.base import Store
from proxystore.store.ref import borrow

store = Store(...)
proxy = store.owned_proxy('value')
borrowed = borrow(proxy)  # (1)!

with ProcessPoolExecutor() as pool:
    future: Future[int] = submit(
        pool.submit,  # (2)!
        args=(sum, borrowed),  # (3)!
    )
    assert future.result() == 6  # (4)!

del proxy  # (5)!

store.close()
```

1. Borrow an [`OwnedProxy`][proxystore.store.ref.OwnedProxy] as a [`RefProxy`][proxystore.store.ref.RefProxy].
2. [`submit()`][proxystore.store.scopes.submit] will call `pool.submit()` with the specified `args` and `kwargs`.
   Here, [`sum`][sum] will be the function invoked on a single argument `borrowed` which is a proxy of a list of integers.
3. The `args` and `kwargs` will be scanned for any proxy reference types, and a callback will be added to the returned future that marks the input proxy references as out-of-scope once the future completes.
4. Once the future is completed, the `borrowed` reference is marked out-of-scope and the reference count of borrows managed internally in `proxy` is decremented.
5. The [`OwnedProxy`][proxystore.store.ref.OwnedProxy], `proxy`, which owns the target value is safe to delete and get garbage collected because there are no remaining reference proxies which have borrowed the target value.
