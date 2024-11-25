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
import copy
import sys
import weakref
from typing import Any
from typing import Callable
from typing import cast
from typing import NoReturn
from typing import SupportsIndex
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import TypeAlias
else:  # pragma: <3.10 cover
    from typing_extensions import TypeAlias

from proxystore.proxy import DefaultClassType
from proxystore.proxy import DefaultHashType
from proxystore.proxy import is_resolved
from proxystore.proxy import Proxy
from proxystore.store.exceptions import ProxyStoreFactoryError
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


def _proxy_trampoline(
    kind: str,
    factory: FactoryType[T],
    default_class: DefaultClassType = None,
    default_hash: DefaultHashType = None,
) -> BaseRefProxy[T]:
    if kind == 'OwnedProxy':
        proxy = OwnedProxy(factory)
    elif kind == 'RefProxy':
        proxy = RefProxy(factory)
    elif kind == 'RefMutProxy':
        proxy = RefMutProxy(factory)
    else:
        raise AssertionError(f'Unknown proxy kind: {kind}')

    object.__setattr__(proxy, '__proxy_default_class__', default_class)
    object.__setattr__(proxy, '__proxy_default_hash__', default_hash)
    return proxy


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

    __slots__ = '__proxy_valid__'

    __proxy_factory__: FactoryType[T]
    __proxy_valid__: bool

    def __init__(
        self,
        factory: FactoryType[T],
        *,
        cache_defaults: bool = False,
        target: T | None = None,
    ) -> None:
        object.__setattr__(self, '__proxy_valid__', True)
        super().__init__(factory, cache_defaults=cache_defaults, target=target)

    @property
    def __proxy_wrapped__(self) -> T:
        if not object.__getattribute__(self, '__proxy_valid__'):
            raise ReferenceInvalidError(
                'Reference has been invalidated. This is likely because it '
                'was pickled and transferred to a different process.',
            )
        return super().__proxy_wrapped__

    @__proxy_wrapped__.deleter
    def __proxy_wrapped__(self) -> None:
        object.__delattr__(self, '__proxy_target__')

    @__proxy_wrapped__.setter
    def __proxy_wrapped__(self, target: T) -> None:
        object.__setattr__(self, '__proxy_target__', target)

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

    def __reduce__(  # type: ignore[override]
        self,
    ) -> tuple[
        Callable[
            [str, FactoryType[T], DefaultClassType, DefaultHashType],
            BaseRefProxy[T],
        ],
        tuple[str, FactoryType[T], DefaultClassType, DefaultHashType],
    ]:
        object.__setattr__(self, '__proxy_valid__', False)
        args = (
            cast(str, type(self).__name__),
            object.__getattribute__(self, '__proxy_factory__'),
            object.__getattribute__(self, '__proxy_default_class__'),
            object.__getattribute__(self, '__proxy_default_hash__'),
        )
        return _proxy_trampoline, args

    def __reduce_ex__(  # type: ignore[override]
        self,
        protocol: SupportsIndex,
    ) -> tuple[
        Callable[
            [str, FactoryType[T], DefaultClassType, DefaultHashType],
            BaseRefProxy[T],
        ],
        tuple[str, FactoryType[T], DefaultClassType, DefaultHashType],
    ]:
        return self.__reduce__()


class OwnedProxy(BaseRefProxy[T]):
    """Represents ownership over an object in a global store.

    This class maintains reference counts of the number of immutable and
    mutable borrows of this proxy. The target object will be evicted from
    the store once this proxy goes out of scope (this is handled via
    `__del__` and an [atexit][atexit] handler).

    Args:
        factory: [`StoreFactory`][proxystore.store.factory.StoreFactory] used
            to resolve the target object from the store.
        cache_defaults: Precompute and cache the `__proxy_default_class__` and
            `__proxy_default_hash__` attributes of the proxy instance from
            `target`. Ignored if `target` is not provided.
        target: Optionally preset the target object.
    """

    __slots__ = (
        '__proxy_finalizer__',
        '__proxy_ref_count__',
        '__proxy_ref_mut_count__',
    )

    __proxy_ref_count__: int
    __proxy_ref_mut_count__: int
    __proxy_finalizer__: Any

    def __init__(
        self,
        factory: FactoryType[T],
        *,
        cache_defaults: bool = False,
        target: T | None = None,
    ) -> None:
        object.__setattr__(self, '__proxy_ref_count__', 0)
        object.__setattr__(self, '__proxy_ref_mut_count__', 0)
        object.__setattr__(
            self,
            '__proxy_finalizer__',
            atexit.register(_WeakRefFinalizer(self, '__del__')),
        )
        super().__init__(factory, cache_defaults=cache_defaults, target=target)

    def __del__(self) -> None:
        atexit.unregister(object.__getattribute__(self, '__proxy_finalizer__'))
        if object.__getattribute__(self, '__proxy_valid__'):
            ref_count = object.__getattribute__(self, '__proxy_ref_count__')
            ref_mut_count = object.__getattribute__(
                self,
                '__proxy_ref_mut_count__',
            )
            if ref_count > 0 or ref_mut_count > 0:
                raise RuntimeError(
                    'Cannot safely delete OwnedProxy because there still '
                    f'exists {ref_count} RefProxy and {ref_mut_count} '
                    'RefMutProxy.',
                )
            factory = object.__getattribute__(self, '__proxy_factory__')
            store = factory.get_store()
            store.evict(factory.key)
            object.__setattr__(self, '__proxy_valid__', False)


class RefProxy(BaseRefProxy[T]):
    """Represents a borrowed reference to an object in the global store.

    Args:
        factory: [`StoreFactory`][proxystore.store.factory.StoreFactory] used
            to resolve the target object from the store.
        cache_defaults: Precompute and cache the `__proxy_default_class__` and
            `__proxy_default_hash__` attributes of the proxy instance from
            `target`. Ignored if `target` is not provided.
        owner: Proxy which has ownership over the target object. This reference
            will keep the owner alive while this borrowed reference is alive.
            In the event this borrowed reference was initialized in a different
            address space from the proxy with ownership, then `owner` will
            be `None`.
        target: Optionally preset the target object.
    """

    __slots__ = ('__proxy_finalizer__', '__proxy_owner__')

    __proxy_finalizer__: Any
    __proxy_owner__: OwnedProxy[T]

    def __init__(
        self,
        factory: FactoryType[T],
        *,
        cache_defaults: bool = False,
        owner: OwnedProxy[T] | None = None,
        target: T | None = None,
    ) -> None:
        object.__setattr__(self, '__proxy_owner__', owner)
        object.__setattr__(
            self,
            '__proxy_finalizer__',
            atexit.register(_WeakRefFinalizer(self, '__del__')),
        )
        super().__init__(factory, cache_defaults=cache_defaults, target=target)

    def __del__(self) -> None:
        atexit.unregister(object.__getattribute__(self, '__proxy_finalizer__'))
        # If owner is None, then this RefMutProxy was likely serialized
        # and sent to a different process. As such, it is the responsibility
        # of that code to take over reference counting.
        owner = object.__getattribute__(self, '__proxy_owner__')
        if owner is not None:
            ref_count = object.__getattribute__(owner, '__proxy_ref_count__')
            assert ref_count >= 1
            object.__setattr__(owner, '__proxy_ref_count__', ref_count - 1)
        object.__setattr__(self, '__proxy_owner__', None)
        object.__setattr__(self, '__proxy_valid__', False)


class RefMutProxy(BaseRefProxy[T]):
    """Represents a borrowed mutable reference to an object in the global store.

    Args:
        factory: [`StoreFactory`][proxystore.store.factory.StoreFactory] used
            to resolve the target object from the store.
        cache_defaults: Precompute and cache the `__proxy_default_class__` and
            `__proxy_default_hash__` attributes of the proxy instance from
            `target`. Ignored if `target` is not provided.
        owner: Proxy which has ownership over the target object. This reference
            will keep the owner alive while this borrowed reference is alive.
            In the event this borrowed reference was initialized in a different
            address space from the proxy with ownership, then `owner` will
            be `None`.
        target: Optionally preset the target object.
    """  # noqa: E501

    __slots__ = ('__proxy_finalizer__', '__proxy_owner__')

    __proxy_finalizer__: Any
    __proxy_owner__: OwnedProxy[T]

    def __init__(
        self,
        factory: FactoryType[T],
        *,
        cache_defaults: bool = False,
        owner: OwnedProxy[T] | None = None,
        target: T | None = None,
    ) -> None:
        object.__setattr__(self, '__proxy_owner__', owner)
        object.__setattr__(
            self,
            '__proxy_finalizer__',
            atexit.register(_WeakRefFinalizer(self, '__del__')),
        )
        super().__init__(factory, cache_defaults=cache_defaults, target=target)

    def __del__(self) -> None:
        atexit.unregister(object.__getattribute__(self, '__proxy_finalizer__'))
        # If owner is None, then this RefMutProxy was likely serialized
        # and sent to a different process. As such, it is the responsibility
        # of that code to take over reference counting.
        owner = object.__getattribute__(self, '__proxy_owner__')
        if owner is not None:
            ref_mut_count = object.__getattribute__(
                owner,
                '__proxy_ref_mut_count__',
            )
            assert ref_mut_count == 1
            object.__setattr__(owner, '__proxy_ref_mut_count__', 0)
        object.__setattr__(self, '__proxy_owner__', None)
        object.__setattr__(self, '__proxy_valid__', False)


def _copy_attributes(
    source: Proxy[T],
    dest: Proxy[T],
    *,
    deepcopy: bool = False,
) -> None:
    if source.__proxy_resolved__:
        target = source.__proxy_wrapped__
        if deepcopy:
            target = copy.deepcopy(target)
        dest.__proxy_wrapped__ = target

    default_class = object.__getattribute__(source, '__proxy_default_class__')
    default_hash = object.__getattribute__(source, '__proxy_default_hash__')

    object.__setattr__(dest, '__proxy_default_class__', default_class)
    object.__setattr__(dest, '__proxy_default_hash__', default_hash)


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
    if object.__getattribute__(proxy, '__proxy_ref_mut_count__') > 0:
        raise MutableBorrowError('Proxy was already borrowed as mutable.')
    object.__setattr__(
        proxy,
        '__proxy_ref_count__',
        object.__getattribute__(proxy, '__proxy_ref_count__') + 1,
    )
    ref_proxy = RefProxy(proxy.__proxy_factory__, owner=proxy)
    if populate_target:
        _copy_attributes(proxy, ref_proxy)
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
    if object.__getattribute__(proxy, '__proxy_ref_mut_count__') > 0:
        raise MutableBorrowError('Proxy was already borrowed as mutable.')
    if object.__getattribute__(proxy, '__proxy_ref_count__') > 0:
        raise MutableBorrowError('Proxy was already borrowed as immutable.')
    object.__setattr__(
        proxy,
        '__proxy_ref_mut_count__',
        object.__getattribute__(proxy, '__proxy_ref_mut_count__') + 1,
    )
    ref_proxy = RefMutProxy(proxy.__proxy_factory__, owner=proxy)
    if populate_target:
        _copy_attributes(proxy, ref_proxy)
    return ref_proxy


def clone(proxy: OwnedProxy[T]) -> OwnedProxy[T]:
    """Clone the target object.

    Creates a new copy of `T` in the global store. If `proxy` is in
    the resolved state, the local version of `T` belonging to `proxy` will
    be deepcopied into the cloned proxy.

    Raises:
        ReferenceNotOwnedError: if `proxy` is not an
            [`OwnedProxy`][proxystore.store.ref.OwnedProxy] instance.
    """
    if not isinstance(proxy, OwnedProxy):
        raise ReferenceNotOwnedError('Only owned references can be cloned.')
    factory = proxy.__proxy_factory__
    store = factory.get_store()
    data = store.connector.get(factory.key)
    new_key = store.connector.put(data)
    new_factory: StoreFactory[Any, T] = StoreFactory(
        new_key,
        store_config=store.config(),
        evict=factory.evict,
        deserializer=factory.deserializer,
    )
    owned = OwnedProxy(new_factory)
    _copy_attributes(proxy, owned, deepcopy=True)
    return owned


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
        ProxyStoreFactoryError: If the proxy's factory is not an instance of
            [`StoreFactory`][proxystore.store.base.StoreFactory].
    """
    if type(proxy) in (OwnedProxy, RefProxy, RefMutProxy):
        # We don't use isinstance to prevent resolving the proxy.
        raise ValueError(
            'Only a base proxy can be converted into an owned proxy.',
        )
    factory = proxy.__proxy_factory__
    if not isinstance(factory, StoreFactory):
        raise ProxyStoreFactoryError(
            'The proxy must contain a factory with type '
            f'{StoreFactory.__name__}. {type(factory).__name__} '
            'is not supported.',
        )
    factory.evict = False
    owned_proxy = OwnedProxy(factory)
    if populate_target:
        _copy_attributes(proxy, owned_proxy)
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
        object.__getattribute__(proxy, '__proxy_ref_mut_count__') > 0
        or object.__getattribute__(proxy, '__proxy_ref_count__') > 0
    ):
        raise MutableBorrowError(
            'OwnedProxy has been borrowed. Cannot mutate.',
        )
    if not is_resolved(proxy):
        return

    store = proxy.__proxy_factory__.get_store()
    try:
        store._set(
            proxy.__proxy_factory__.key,
            proxy.__proxy_wrapped__,
            serializer=serializer,
        )
    except NotImplementedError as e:  # pragma: no cover
        raise NotImplementedError(
            'Mutating the global copy of the value requires a connector '
            'type that supports set().',
        ) from e
