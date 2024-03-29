"""Object ownership and borrowing with proxies.

Warning:
    These features are experimental and may change in future releases.

This module implements Rust-like ownership and borrowing rules for Python
objects in shared memory using transparent object proxies. Thus, these
proxy reference types are similar to the type returned by
[`Store.proxy()`][proxystore.store.base.Store.proxy]---they will resolve
to an object in the global store. However, these proxy references enforce
additional rules.

1. Each *target* object of type `T` in the global store has an associated
   [`OwnedProxy[T]`][proxystore.store.ref.OwnedProxy].
2. There can only be one [`OwnedProxy[T]`][proxystore.store.ref.OwnedProxy]
   for any *target* in the global store.
3. When an [`OwnedProxy[T]`][proxystore.store.ref.OwnedProxy] goes out of
   scope (e.g., gets garbage collected), the associated *target* is removed
   from the global store.

Tip:
    The docstrings often use `T` to refer to both the *target* object in the
    global store and the type of the *target* object.

An [`OwnedProxy[T]`][proxystore.store.ref.OwnedProxy] can be borrowed without
relinquishing ownership. This requires two additional rules.

1. At any given time, you can have either one mutable reference to the
   *target*, a [`RefMutProxy[T]`][proxystore.store.ref.RefMutProxy], or
   any number of immutable references, a
   [`RefProxy[T]`][proxystore.store.ref.RefProxy].
2. References must always be valid. I.e., you cannot delete an
   [`OwnedProxy[T]`][proxystore.store.ref.OwnedProxy] while it has been
   borrowed via a [`RefProxy[T]`][proxystore.store.ref.RefProxy] or
   [`RefMutProxy[T]`][proxystore.store.ref.RefMutProxy].

All three reference types ([`OwnedProxy[T]`][proxystore.store.ref.OwnedProxy],
[`RefProxy[T]`][proxystore.store.ref.RefProxy], and
[`RefMutProxy[T]`][proxystore.store.ref.RefMutProxy]) behave like an instance
of `T`, forwarding operations on themselves to a locally cached instance of
`T`.
"""

from __future__ import annotations

import atexit
import sys
import weakref
from typing import Any
from typing import Callable
from typing import NoReturn
from typing import SupportsIndex
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import TypeAlias
else:  # pragma: <3.10 cover
    from typing_extensions import TypeAlias

from proxystore.proxy import is_resolved
from proxystore.proxy import Proxy
from proxystore.store.factory import StoreFactory
from proxystore.store.types import SerializerT

T = TypeVar('T')
FactoryType: TypeAlias = StoreFactory[Any, T]


class BaseRefProxyError(Exception):
    """Base exception type for proxy references."""

    pass


class MutableBorrowError(BaseRefProxyError):
    """Exception raised when violating borrowing rules."""

    pass


class ReferenceNotOwnedError(BaseRefProxyError):
    """Exception raised when invoking an operation on a non-owned reference."""

    pass


class ReferenceInvalidError(BaseRefProxyError):
    """Exception raised when a reference instance has been invalidated."""

    pass


class _WeakRefFinalizer:
    def __init__(self, obj: Any, method: str) -> None:
        self.wr = weakref.ref(obj)
        self.method = method

    def __call__(self) -> None:
        obj = self.wr()
        if obj is not None:
            getattr(obj, self.method)()


class BaseRefProxy(Proxy[T]):
    """Base reference proxy type.

    This base type adds some features to [`Proxy`][proxystore.proxy.Proxy]
    that are shared by all the reference types:

    1. Valid flag. When invalidated, the proxy will raise a
       [`ReferenceInvalidError`][proxystore.store.ref.ReferenceInvalidError]
       when accessing the wrapped target object.
    2. Disable [`copy()`][copy.copy] and [`deepcopy()`][copy.deepcopy]
       support to prevent misuse of the API. Generally,
       [`borrow()`][proxystore.store.ref.borrow] or
       [`clone()`][proxystore.store.ref.clone] should be used instead.
    """

    def __init__(self, factory: FactoryType[T]) -> None:
        object.__setattr__(self, '__valid__', True)
        super().__init__(factory)

    @property
    def __wrapped__(self) -> T:
        if not object.__getattribute__(self, '__valid__'):
            raise ReferenceInvalidError(
                'Reference has been invalidated. This is likely because it '
                'was pickled and transferred to a different process.',
            )
        return super().__wrapped__

    @__wrapped__.deleter
    def __wrapped__(self) -> None:
        object.__delattr__(self, '__target__')

    @__wrapped__.setter
    def __wrapped__(self, target: T) -> None:
        object.__setattr__(self, '__target__', target)

    def __copy__(self) -> NoReturn:
        raise NotImplementedError(
            'Copy is not implemented for reference proxy types to avoid '
            'incidental misuse of the API. Use clone() instead.',
        )

    def __deepcopy__(self, memo: dict[Any, Any]) -> NoReturn:
        raise NotImplementedError(
            'Deep copy is not implemented for reference proxy types to avoid '
            'incidental misuse of the API. Use clone() instead.',
        )


def _owned_proxy_trampoline(factory: FactoryType[T]) -> OwnedProxy[T]:
    # See proxystore.proxy._proxy_trampoline for purpose
    return OwnedProxy(factory)


class OwnedProxy(BaseRefProxy[T]):
    """Represents ownership over an object in a global store.

    This class maintains reference counts of the number of immutable and
    mutable borrows of this proxy. The target object will be evicted from
    the store once this proxy goes out of scope (this is handled via
    `__del__` and an [atexit][atexit] handler).

    Args:
        factory: [`StoreFactory`][proxystore.store.factory.StoreFactory] used
            to resolve the target object from the store.
    """

    def __init__(self, factory: FactoryType[T]) -> None:
        object.__setattr__(self, '__ref_count__', 0)
        object.__setattr__(self, '__ref_mut_count__', 0)
        object.__setattr__(
            self,
            '__finalizer__',
            atexit.register(_WeakRefFinalizer(self, '__del__')),
        )
        super().__init__(factory)

    def __del__(self) -> None:
        atexit.unregister(object.__getattribute__(self, '__finalizer__'))
        if object.__getattribute__(self, '__valid__'):
            ref_count = object.__getattribute__(self, '__ref_count__')
            ref_mut_count = object.__getattribute__(self, '__ref_mut_count__')
            if ref_count > 0 or ref_mut_count > 0:
                raise RuntimeError(
                    'Cannot safely delete OwnedProxy because there still '
                    f'exists {ref_count} RefProxy and {ref_mut_count} '
                    'RefMutProxy.',
                )
            factory = object.__getattribute__(self, '__factory__')
            store = factory.get_store()
            store.evict(factory.key)
            object.__setattr__(self, '__valid__', False)

    def __reduce__(  # type: ignore[override]
        self,
    ) -> tuple[
        Callable[[FactoryType[T]], OwnedProxy[T]],
        tuple[FactoryType[T]],
    ]:
        object.__setattr__(self, '__valid__', False)
        return _owned_proxy_trampoline, (
            object.__getattribute__(self, '__factory__'),
        )

    def __reduce_ex__(  # type: ignore[override]
        self,
        protocol: SupportsIndex,
    ) -> tuple[
        Callable[[FactoryType[T]], OwnedProxy[T]],
        tuple[FactoryType[T]],
    ]:
        return self.__reduce__()


def _ref_proxy_trampoline(factory: FactoryType[T]) -> RefProxy[T]:
    # See proxystore.proxy._proxy_trampoline for purpose
    return RefProxy(factory)


class RefProxy(BaseRefProxy[T]):
    """Represents a borrowed reference to an object in the global store.

    Args:
        factory: [`StoreFactory`][proxystore.store.factory.StoreFactory] used
            to resolve the target object from the store.
        owner: Proxy which has ownership over the target object. This reference
            will keep the owner alive while this borrowed reference is alive.
            In the event this borrowed reference was initialized in a different
            address space from the proxy with ownership, then `owner` will
            be `None`.
    """

    def __init__(
        self,
        factory: FactoryType[T],
        *,
        owner: OwnedProxy[T] | None = None,
    ) -> None:
        object.__setattr__(self, '__owner__', owner)
        object.__setattr__(
            self,
            '__finalizer__',
            atexit.register(_WeakRefFinalizer(self, '__del__')),
        )
        super().__init__(factory)

    def __del__(self) -> None:
        atexit.unregister(object.__getattribute__(self, '__finalizer__'))
        # If owner is None, then this RefMutProxy was likely serialized
        # and sent to a different process. As such, it is the responsibility
        # of that code to take over reference counting.
        owner = object.__getattribute__(self, '__owner__')
        if owner is not None:
            ref_count = object.__getattribute__(owner, '__ref_count__')
            assert ref_count >= 1
            object.__setattr__(owner, '__ref_count__', ref_count - 1)
        object.__setattr__(self, '__owner__', None)
        object.__setattr__(self, '__valid__', False)

    def __reduce__(  # type: ignore[override]
        self,
    ) -> tuple[Callable[[FactoryType[T]], RefProxy[T]], tuple[FactoryType[T]]]:
        object.__setattr__(self, '__valid__', False)
        return _ref_proxy_trampoline, (
            object.__getattribute__(self, '__factory__'),
        )

    def __reduce_ex__(  # type: ignore[override]
        self,
        protocol: SupportsIndex,
    ) -> tuple[Callable[[FactoryType[T]], RefProxy[T]], tuple[FactoryType[T]]]:
        return self.__reduce__()


def _ref_mut_proxy_trampoline(factory: FactoryType[T]) -> RefMutProxy[T]:
    # See proxystore.proxy._proxy_trampoline for purpose
    return RefMutProxy(factory)


class RefMutProxy(BaseRefProxy[T]):
    """Represents a borrowed mutable reference to an object in the global store.

    Args:
        factory: [`StoreFactory`][proxystore.store.factory.StoreFactory] used
            to resolve the target object from the store.
        owner: Proxy which has ownership over the target object. This reference
            will keep the owner alive while this borrowed reference is alive.
            In the event this borrowed reference was initialized in a different
            address space from the proxy with ownership, then `owner` will
            be `None`.
    """  # noqa: E501

    def __init__(
        self,
        factory: FactoryType[T],
        *,
        owner: OwnedProxy[T] | None = None,
    ) -> None:
        object.__setattr__(self, '__owner__', owner)
        object.__setattr__(
            self,
            '__finalizer__',
            atexit.register(_WeakRefFinalizer(self, '__del__')),
        )
        super().__init__(factory)

    def __del__(self) -> None:
        atexit.unregister(object.__getattribute__(self, '__finalizer__'))
        # If owner is None, then this RefMutProxy was likely serialized
        # and sent to a different process. As such, it is the responsibility
        # of that code to take over reference counting.
        owner = object.__getattribute__(self, '__owner__')
        if owner is not None:
            ref_mut_count = object.__getattribute__(
                owner,
                '__ref_mut_count__',
            )
            assert ref_mut_count == 1
            object.__setattr__(owner, '__ref_mut_count__', 0)
        object.__setattr__(self, '__owner__', None)
        object.__setattr__(self, '__valid__', False)

    def __reduce__(  # type: ignore[override]
        self,
    ) -> tuple[
        Callable[[FactoryType[T]], RefMutProxy[T]],
        tuple[FactoryType[T]],
    ]:
        object.__setattr__(self, '__valid__', False)
        return _ref_mut_proxy_trampoline, (
            object.__getattribute__(self, '__factory__'),
        )

    def __reduce_ex__(  # type: ignore[override]
        self,
        protocol: SupportsIndex,
    ) -> tuple[
        Callable[[FactoryType[T]], RefMutProxy[T]],
        tuple[FactoryType[T]],
    ]:
        return self.__reduce__()


def borrow(
    proxy: OwnedProxy[T],
    *,
    populate_target: bool = True,
) -> RefProxy[T]:
    """Borrow `T` by creating an immutable reference of `T`.

    Note:
        This mutates `proxy`.

    Args:
        proxy: Proxy reference to borrow.
        populate_target: If the target of `proxy` has already been resolved,
            copy the reference to the target into the returned proxy such that
            the returned proxy is already resolved.

    Raises:
        ReferenceNotOwnedError: if `proxy` is not an
            [`OwnedProxy`][proxystore.store.ref.OwnedProxy] instance.
        MutableBorrowError: if `proxy` has already been mutably borrowed.
    """
    if not isinstance(proxy, OwnedProxy):
        raise ReferenceNotOwnedError('Only owned references can be borrowed.')
    if object.__getattribute__(proxy, '__ref_mut_count__') > 0:
        raise MutableBorrowError('Proxy was already borrowed as mutable.')
    object.__setattr__(
        proxy,
        '__ref_count__',
        object.__getattribute__(proxy, '__ref_count__') + 1,
    )
    ref_proxy = RefProxy(proxy.__factory__, owner=proxy)
    if populate_target and is_resolved(proxy):
        ref_proxy.__wrapped__ = proxy.__wrapped__
    return ref_proxy


def mut_borrow(
    proxy: OwnedProxy[T],
    *,
    populate_target: bool = True,
) -> RefMutProxy[T]:
    """Mutably borrow `T` by creating an mutable reference of `T`.

    Note:
        This mutates `proxy`.

    Args:
        proxy: Proxy reference to borrow.
        populate_target: If the target of `proxy` has already been resolved,
            copy the reference to the target into the returned proxy such that
            the returned proxy is already resolved.

    Raises:
        ReferenceNotOwnedError: if `proxy` is not an
            [`OwnedProxy`][proxystore.store.ref.OwnedProxy] instance.
        MutableBorrowError: if `proxy` has already been borrowed
            (mutable/immutable).
    """
    if not isinstance(proxy, OwnedProxy):
        raise ReferenceNotOwnedError('Only owned references can be borrowed.')
    if object.__getattribute__(proxy, '__ref_mut_count__') > 0:
        raise MutableBorrowError('Proxy was already borrowed as mutable.')
    if object.__getattribute__(proxy, '__ref_count__') > 0:
        raise MutableBorrowError('Proxy was already borrowed as immutable.')
    object.__setattr__(
        proxy,
        '__ref_mut_count__',
        object.__getattribute__(proxy, '__ref_mut_count__') + 1,
    )
    ref_proxy = RefMutProxy(proxy.__factory__, owner=proxy)
    if populate_target and is_resolved(proxy):
        ref_proxy.__wrapped__ = proxy.__wrapped__
    return ref_proxy


def clone(proxy: OwnedProxy[T]) -> OwnedProxy[T]:
    """Clone the target object.

    Creates a new copy of `T` in the global store.

    Raises:
        ReferenceNotOwnedError: if `proxy` is not an
            [`OwnedProxy`][proxystore.store.ref.OwnedProxy] instance.
    """
    if not isinstance(proxy, OwnedProxy):
        raise ReferenceNotOwnedError('Only owned references can be cloned.')
    factory = proxy.__factory__
    store = factory.get_store()
    data = store.connector.get(factory.key)
    new_key = store.connector.put(data)
    new_factory: StoreFactory[Any, T] = StoreFactory(
        new_key,
        store_config=store.config(),
        evict=factory.evict,
        deserializer=factory.deserializer,
    )
    return OwnedProxy(new_factory)


def into_owned(
    proxy: Proxy[T],
    *,
    populate_target: bool = True,
) -> OwnedProxy[T]:
    """Convert a basic proxy into an owned proxy.

    Warning:
        It is the caller's responsibility to ensure that `proxy` has not been
        copied already.

    Note:
        This will unset the `evict` flag on the proxy.

    Args:
        proxy: Proxy reference to borrow.
        populate_target: If the target of `proxy` has already been resolved,
            copy the reference to the target into the returned proxy such that
            the returned proxy is already resolved.

    Raises:
        ValueError: if `proxy` is already a
            [`BaseRefProxy`][proxystore.store.ref.BaseRefProxy] instance.
    """
    if type(proxy) in (OwnedProxy, RefProxy, RefMutProxy):
        # We don't use isinstance to prevent resolving the proxy.
        raise ValueError(
            'Only a base proxy can be converted into an owned proxy.',
        )
    factory = proxy.__factory__
    factory.evict = False
    owned_proxy = OwnedProxy(factory)
    if populate_target and is_resolved(proxy):
        owned_proxy.__wrapped__ = proxy.__wrapped__
    return owned_proxy


def update(
    proxy: OwnedProxy[T] | RefMutProxy[T],
    *,
    serializer: SerializerT | None = None,
) -> None:
    """Update the global copy of the target.

    Note:
        If the proxy has not been resolved, there is nothing to update and
        this function is a no-op.

    Warning:
        This will not invalidate already cached copies of the global target.

    Args:
        proxy: Proxy containing a modified local copy of the target to use
            as the new global value.
        serializer: Optionally override the default serializer for the
                store instance when pushing the local copy to the store.

    Raises:
        MutableBorrowError: if `proxy` has been mutably borrowed.
        NotImplementedError: if the `connector` of the `store` used to create
            the proxy does not implement the
            [`DeferrableConnector`][proxystore.connectors.protocols.DeferrableConnector]
            protocol.
        ReferenceNotOwnedError: if `proxy` is not an
            [`OwnedProxy`][proxystore.store.ref.OwnedProxy] or
            [`RefMutProxy`][proxystore.store.ref.RefMutProxy] instance.
    """
    if not isinstance(proxy, (OwnedProxy, RefMutProxy)):
        raise ReferenceNotOwnedError('Reference is an immutable borrow.')
    if isinstance(proxy, OwnedProxy) and (
        object.__getattribute__(proxy, '__ref_mut_count__') > 0
        or object.__getattribute__(proxy, '__ref_count__') > 0
    ):
        raise MutableBorrowError(
            'OwnedProxy has been borrowed. Cannot mutate.',
        )
    if not is_resolved(proxy):
        return

    store = proxy.__factory__.get_store()
    try:
        store._set(
            proxy.__factory__.key,
            proxy.__wrapped__,
            serializer=serializer,
        )
    except NotImplementedError as e:  # pragma: no cover
        raise NotImplementedError(
            'Mutating the global copy of the value requires a connector '
            'type that supports set().',
        ) from e
