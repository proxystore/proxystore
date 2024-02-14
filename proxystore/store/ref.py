from __future__ import annotations

import sys
from typing import Any
from typing import Callable
from typing import NoReturn
from typing import SupportsIndex
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import TypeAlias
else:  # pragma: <3.10 cover
    from typing_extensions import TypeAlias

from proxystore.proxy import Proxy
from proxystore.store.factory import StoreFactory

T = TypeVar('T')
FactoryType: TypeAlias = StoreFactory[Any, T]


class ReferenceInvalidError(Exception):
    pass


class BaseRefProxy(Proxy[T]):
    def __init__(self, factory: FactoryType[T]) -> None:
        self.__valid__ = True
        super().__init__(factory)

    @property
    def __wrapped__(self) -> T:
        if not self.__valid__:
            raise ReferenceInvalidError
        return super().__wrapped__

    def __copy__(self) -> NoReturn:
        raise NotImplementedError

    def __deepcopy__(self, memo: dict[Any, Any]) -> NoReturn:
        raise NotImplementedError


def _owned_proxy_trampoline(factory: FactoryType[T]) -> OwnedProxy[T]:
    # See proxystore.proxy._proxy_trampoline for purpose
    return OwnedProxy(factory)


class OwnedProxy(BaseRefProxy[T]):
    def __init__(self, factory: FactoryType[T]) -> None:
        super().__init__(factory)

    def __reduce__(  # type: ignore[override]
        self,
    ) -> tuple[
        Callable[[FactoryType[T]], OwnedProxy[T]],
        tuple[FactoryType[T]],
    ]:
        self.__valid__ = False
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
    def __init__(self, factory: FactoryType[T]) -> None:
        super().__init__(factory)

    def __reduce__(  # type: ignore[override]
        self,
    ) -> tuple[Callable[[FactoryType[T]], RefProxy[T]], tuple[FactoryType[T]]]:
        self.__valid__ = False
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
    def __init__(self, factory: FactoryType[T]) -> None:
        super().__init__(factory)

    def __reduce__(  # type: ignore[override]
        self,
    ) -> tuple[
        Callable[[FactoryType[T]], RefMutProxy[T]],
        tuple[FactoryType[T]],
    ]:
        self.__valid__ = False
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


def borrow(p: OwnedProxy[T]) -> RefProxy[T]:
    """Borrow T by creating an immutable reference of T.

    Note:
        This mutates `proxy`.

    Raises:
        TypeError: if `proxy` is not an
            [`OwnedProxy`][proxystore.store.ref.OwnedProxy] instance.
        RuntimeError: if `proxy` has already been mutably borrowed.
    """
    ...


def mut_borrow(p: OwnedProxy[T]) -> RefMutProxy[T]:
    """Mutably borrow T by creating an mutable reference of T.

    Note:
        This mutates `proxy`.

    Raises:
        TypeError: if `proxy` is not an
            [`OwnedProxy`][proxystore.store.ref.OwnedProxy] instance.
        RuntimeError: if `proxy` has already been borrowed (mutable/immutable).
    """
    ...


def clone(p: OwnedProxy[T]) -> OwnedProxy[T]:
    """Clone the target object.

    Creates a new copy of T in the global store.

    Raises:
        TypeError: if `proxy` is not an
            [`OwnedProxy`][proxystore.store.ref.OwnedProxy] instance.
    """
    ...


def update(proxy: OwnedProxy[T] | RefMutProxy[T]) -> None:
    """Update the global copy of the target.

    Raises:
        TypeError: if `proxy` is not an
            [`OwnedProxy`][proxystore.store.ref.OwnedProxy] or
            [`RefMutProxy`][proxystore.store.ref.RefMutProxy] instance.
        RuntimeError: if `proxy` has been mutably borrowed.
    """
    ...
