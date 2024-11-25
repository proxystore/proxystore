# This package contains source code from python-lazy-object-proxy v1.10.0
# which is available under the BSD 2-Clause License included below.
#
# The following modifications to the source has been made:
#   * Replaced certain uses of @property with a custom @proxy_property.
#   * Altered pickling behaviour.
#   * Consolidated source from multiple modules into this single module.
#   * Altered docstrings.
#   * Code formatting and additional type annotations.
#
# Source: https://github.com/ionelmc/python-lazy-object-proxy/tree/v1.10.0
#
# BSD 2-Clause License
#
# Copyright (c) 2014-2023, Ionel Cristian Mărieș. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
"""Proxy implementation and helpers."""

from __future__ import annotations

import operator
import sys
from collections.abc import Awaitable
from collections.abc import Iterator
from inspect import CO_ITERABLE_COROUTINE
from types import CoroutineType
from types import GeneratorType
from typing import Any
from typing import Callable
from typing import cast
from typing import Generic
from typing import Optional
from typing import SupportsIndex
from typing import TypeVar
from typing import Union

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import TypeAlias
else:  # pragma: <3.10 cover
    from typing_extensions import TypeAlias

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

if sys.version_info >= (3, 12):  # pragma: >=3.12 cover
    from typing import TypeAliasType
else:  # pragma: <3.12 cover
    from typing_extensions import TypeAliasType

from proxystore.proxy._utils import _do_await
from proxystore.proxy._utils import _do_yield_from
from proxystore.proxy._utils import as_metaclass
from proxystore.proxy._utils import ProxyMetaType

T = TypeVar('T')
FactoryType: TypeAlias = Callable[[], T]
DefaultClassType: TypeAlias = Optional[type]
DefaultHashType: TypeAlias = Optional[Union[Exception, int]]


def _proxy_trampoline(
    factory: FactoryType[T],
    default_class: DefaultClassType = None,
    default_hash: DefaultHashType = None,
) -> Proxy[T]:
    proxy = Proxy(factory)
    object.__setattr__(proxy, '__proxy_default_class__', default_class)
    object.__setattr__(proxy, '__proxy_default_hash__', default_hash)
    return proxy


class Proxy(as_metaclass(ProxyMetaType), Generic[T]):  # type: ignore[misc]
    """Lazy object proxy.

    An extension of the Proxy from
    [lazy-object-proxy](https://github.com/ionelmc/python-lazy-object-proxy){target=_blank}
    with modified attribute lookup and pickling behavior.

    An object proxy acts as a thin wrapper around a Python object, i.e.
    the proxy behaves identically to the underlying object. The proxy is
    initialized with a callable *factory* object. The factory returns the
    underlying object when called, i.e. *resolves* the proxy. This means a
    proxy performs lazy/just-in-time resolution, i.e., the proxy
    does not call the factory until the first access to the proxy.

    ```python linenums="1"
    from proxystore.proxy import Proxy

    def factory() -> list[int]:
        return [1, 2, 3]

    proxy = Proxy(factory)
    assert isinstance(proxy, list)
    assert proxy == [1, 2, 3]
    ```

    Note:
        The `factory`, by default, is only ever called once during the
        lifetime of a proxy instance.

    Note:
        When a proxy instance is pickled, only the `factory` is pickled, not
        the wrapped object. Thus, proxy instances can be pickled and passed
        around cheaply, and once the proxy is unpickled and used, the `factory`
        will be called again to resolve the object.

    Tip:
        Common data structures (e.g., [`dict`][dict] or [`set`][set]) and
        operations (e.g., [`isinstance`][isinstance]) will resolve an
        unresolved proxy. This can result in unintentional performance
        degradation for expensive factories, such as those that require
        significant I/O or produce target objects that require a lot of memory.
        The `target` and `cache_defaults` parameters of
        [`Proxy`][proxystore.proxy.Proxy] can prevent these unintenional
        proxy resolves by caching the `__class__` and `__hash__` values of the
        target object in the proxy.

        ```python linenums="1"
        from proxystore.proxy import Proxy
        from proxystore.proxy import is_resolved

        proxy = Proxy(lambda: 'value')
        assert not is_resolved(proxy)

        assert isinstance(proxy, str)  # (1)!
        assert is_resolved(proxy)

        value = 'value'
        proxy = Proxy(lambda: value, cache_defaults=True, target=value)  # (2)!
        assert not is_resolved(proxy)

        assert isinstance(proxy, str)  # (3)!
        assert not is_resolved(proxy)
        ```

        1. Using [`isinstance`][isinstance] calls `__class__` on the target
           object which requires the proxy to be resolved. In many cases,
           it may be desirable to check the type of a proxy's target object
           without incurring the cost of resolving the target.
        2. If the target is available when constructing the proxy, the
           proxy can precompute and cache the `__class__` and `__hash__` values
           of the target.
        3. Using [`isinstance`][isinstance] no longer requires the proxy
           to be resolved, instead using the precomputed value.

    Warning:
        A proxy of a singleton type (e.g., `True`, `False`, and `None`) will
        not behave exactly as a singleton type would. This is because the
        proxy itself is not a singleton.

        ```python
        >>> from proxystore.proxy import Proxy
        >>> p = Proxy(lambda: True)
        >>> p == True
        True
        >>> p is True
        False
        ```

    Warning:
        Python bindings to other languages (e.g., C, C++) may throw type
        errors when receiving a [`Proxy`][proxystore.proxy.Proxy] instance.
        Casting the proxy or extracting the target object may be needed.

        ```python
        >>> import io
        >>> from proxystore.proxy import Proxy
        >>> s = 'mystring'
        >>> p = Proxy(lambda: s)
        >>> io.StringIO(p)
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        TypeError: initial_value must be str or None, not Proxy
        >>> io.StringIO(str(p))  # succeeds
        ```

    Attributes:
        __proxy_factory__: Factory function which resolves to the target
            object.
        __proxy_target__: The target object once resolved.
        __proxy_resolved__: `True` if `__proxy_target__` is set.
        __proxy_wrapped__: A property that either returns `__proxy_target__`
            if it exists else calls `__proxy_factory__`, saving the result to
            `__proxy_target__` and returning said result.
        __proxy_default_class__: Optional default class type value to use when
            a proxy is in the unresolved state. This avoids needing to resolve
            the proxy to perform [`isinstance`][isinstance] checks. This value
            is always ignored while the proxy is resolved because `__class__`
            is a writable property of the cached target and could be altered.
        __proxy_default_hash__: Optional default hash value to use when a proxy
            is in the unresolved state and [`hash()`][hash] is called. This
            avoids needing to resolve the proxy for simple operations like
            dictionary updates. This value is always ignored while the proxy
            is resolved because the cached target may be modified which
            can alter the value of the hash.

    Args:
        factory: Callable object that returns the underlying object when
            called. The factory should be pure meaning that every call
            of the factory returns the same object.
        cache_defaults: Precompute and cache the `__proxy_default_class__` and
            `__proxy_default_hash__` attributes of the proxy instance from
            `target`. Ignored if `target` is not provided.
        target: Optionally preset the target object.

    Raises:
        TypeError: If `factory` is not callable.
    """

    __slots__ = (
        '__proxy_default_class__',
        '__proxy_default_hash__',
        '__proxy_factory__',
        '__proxy_target__',
    )

    __proxy_target__: T
    __proxy_factory__: FactoryType[T]
    __proxy_default_class__: DefaultClassType
    __proxy_default_hash__: DefaultHashType

    def __init__(
        self,
        factory: FactoryType[T],
        *,
        cache_defaults: bool = False,
        target: T | None = None,
    ) -> None:
        if not callable(factory):
            raise TypeError('Factory must be callable.')
        object.__setattr__(self, '__proxy_factory__', factory)

        default_class: DefaultClassType = None
        default_hash: DefaultHashType = None

        if target is not None:
            object.__setattr__(self, '__proxy_target__', target)
            if cache_defaults:
                default_class = target.__class__
                try:
                    default_hash = hash(target)
                except TypeError as e:
                    default_hash = e

        object.__setattr__(self, '__proxy_default_class__', default_class)
        object.__setattr__(self, '__proxy_default_hash__', default_hash)

    @property
    def __proxy_resolved__(self) -> bool:
        try:
            object.__getattribute__(self, '__proxy_target__')
        except AttributeError:
            return False
        else:
            return True

    @property
    def __proxy_wrapped__(self) -> T:
        try:
            return cast(T, object.__getattribute__(self, '__proxy_target__'))
        except AttributeError:
            try:
                factory = object.__getattribute__(self, '__proxy_factory__')
            except AttributeError as exc:
                raise ValueError(
                    'Proxy is not initialized: __proxy_factory__ is missing.',
                ) from exc
            target = factory()
            object.__setattr__(self, '__proxy_target__', target)
            return target

    @__proxy_wrapped__.deleter
    def __proxy_wrapped__(self) -> None:
        object.__delattr__(self, '__proxy_target__')

    @__proxy_wrapped__.setter
    def __proxy_wrapped__(self, target: T) -> None:
        object.__setattr__(self, '__proxy_target__', target)

    @property
    def __name__(self) -> str:
        return self.__proxy_wrapped__.__name__  # type: ignore[attr-defined]

    @__name__.setter
    def __name__(self, value: str) -> None:
        self.__proxy_wrapped__.__name__ = value  # type: ignore[attr-defined]

    @property
    def __class__(self) -> Any:
        default = object.__getattribute__(self, '__proxy_default_class__')
        if not self.__proxy_resolved__ and default is not None:
            return default
        else:
            return self.__proxy_wrapped__.__class__

    @__class__.setter
    def __class__(self, value: Any) -> None:  # pragma: no cover
        self.__proxy_wrapped__.__class__ = value

    def __dir__(self) -> Any:
        return dir(self.__proxy_wrapped__)

    def __str__(self) -> str:
        return str(self.__proxy_wrapped__)

    def __bytes__(self) -> bytes:
        return bytes(self.__proxy_wrapped__)  # type: ignore[call-overload]

    def __repr__(self) -> str:
        try:
            target = object.__getattribute__(self, '__proxy_target__')
        except AttributeError:
            return (
                f'<{type(self).__name__} at 0x{id(self):x} with '
                f'factory {self.__proxy_factory__!r}>'
            )
        else:
            return (
                f'<{type(self).__name__} at 0x{id(self):x} '
                f'wrapping {target!r} at 0x{id(target):x} with '
                f'factory {self.__proxy_factory__!r}>'
            )

    def __fspath__(self) -> Any:
        wrapped = self.__proxy_wrapped__
        if isinstance(wrapped, (bytes, str)):
            return wrapped
        else:
            fspath = getattr(wrapped, '__fspath__', None)
            if fspath is None:
                return wrapped
            else:
                return fspath()

    def __reversed__(self) -> Any:
        return reversed(self.__proxy_wrapped__)  # type: ignore[call-overload]

    def __round__(self) -> Any:
        return round(self.__proxy_wrapped__)  # type: ignore[call-overload]

    def __lt__(self, other: Any) -> bool:
        return self.__proxy_wrapped__ < other

    def __le__(self, other: Any) -> bool:
        return self.__proxy_wrapped__ <= other

    def __eq__(self, other: Any) -> bool:
        return self.__proxy_wrapped__ == other

    def __ne__(self, other: Any) -> bool:
        return self.__proxy_wrapped__ != other

    def __gt__(self, other: Any) -> bool:
        return self.__proxy_wrapped__ > other

    def __ge__(self, other: Any) -> bool:
        return self.__proxy_wrapped__ >= other

    def __hash__(self) -> int:
        default = object.__getattribute__(self, '__proxy_default_hash__')
        if not self.__proxy_resolved__ and default is not None:
            if isinstance(default, Exception):
                raise default
            else:
                return default
        else:
            return hash(self.__proxy_wrapped__)

    def __bool__(self) -> bool:
        return bool(self.__proxy_wrapped__)

    def __setattr__(self, name: str, value: Any) -> None:
        if hasattr(type(self), name):
            object.__setattr__(self, name, value)
        else:
            setattr(self.__proxy_wrapped__, name, value)

    def __getattr__(self, name: str) -> Any:
        if name in ('__proxy_wrapped__', '__proxy_factory__'):
            raise AttributeError(name)
        else:
            return getattr(self.__proxy_wrapped__, name)

    def __delattr__(self, name: str) -> None:
        if hasattr(type(self), name):
            object.__delattr__(self, name)
        else:
            delattr(self.__proxy_wrapped__, name)

    def __add__(self, other: Any) -> Any:
        return self.__proxy_wrapped__ + other

    def __sub__(self, other: Any) -> Any:
        return self.__proxy_wrapped__ - other

    def __mul__(self, other: Any) -> Any:
        return self.__proxy_wrapped__ * other

    def __matmul__(self, other: Any) -> Any:
        return self.__proxy_wrapped__ @ other

    def __truediv__(self, other: Any) -> Any:
        return operator.truediv(self.__proxy_wrapped__, other)

    def __floordiv__(self, other: Any) -> Any:
        return self.__proxy_wrapped__ // other

    def __mod__(self, other: Any) -> Any:
        return self.__proxy_wrapped__ % other

    def __divmod__(self, other: Any) -> Any:
        return divmod(self.__proxy_wrapped__, other)

    def __pow__(self, other: Any, *args: Any) -> Any:
        return pow(self.__proxy_wrapped__, other, *args)  # type: ignore[call-overload]

    def __lshift__(self, other: Any) -> Any:
        return self.__proxy_wrapped__ << other

    def __rshift__(self, other: Any) -> Any:
        return self.__proxy_wrapped__ >> other

    def __and__(self, other: Any) -> Any:
        return self.__proxy_wrapped__ & other

    def __xor__(self, other: Any) -> Any:
        return self.__proxy_wrapped__ ^ other

    def __or__(self, other: Any) -> Any:
        return self.__proxy_wrapped__ | other

    def __radd__(self, other: Any) -> Any:
        return other + self.__proxy_wrapped__

    def __rsub__(self, other: Any) -> Any:
        return other - self.__proxy_wrapped__

    def __rmul__(self, other: Any) -> Any:
        return other * self.__proxy_wrapped__

    def __rmatmul__(self, other: Any) -> Any:
        return other @ self.__proxy_wrapped__

    def __rtruediv__(self, other: Any) -> Any:
        return operator.truediv(other, self.__proxy_wrapped__)

    def __rfloordiv__(self, other: Any) -> Any:
        return other // self.__proxy_wrapped__

    def __rmod__(self, other: Any) -> Any:
        return other % self.__proxy_wrapped__

    def __rdivmod__(self, other: Any) -> Any:
        return divmod(other, self.__proxy_wrapped__)

    def __rpow__(self, other: Any, *args: Any) -> Any:
        return pow(other, self.__proxy_wrapped__, *args)

    def __rlshift__(self, other: Any) -> Any:
        return other << self.__proxy_wrapped__

    def __rrshift__(self, other: Any) -> Any:
        return other >> self.__proxy_wrapped__

    def __rand__(self, other: Any) -> Any:
        return other & self.__proxy_wrapped__

    def __rxor__(self, other: Any) -> Any:
        return other ^ self.__proxy_wrapped__

    def __ror__(self, other: Any) -> Any:
        return other | self.__proxy_wrapped__

    def __iadd__(self, other: Any) -> Self:
        self.__proxy_wrapped__ += other
        return self

    def __isub__(self, other: Any) -> Self:
        self.__proxy_wrapped__ -= other
        return self

    def __imul__(self, other: Any) -> Self:
        self.__proxy_wrapped__ *= other
        return self

    def __imatmul__(self, other: Any) -> Self:
        self.__proxy_wrapped__ @= other
        return self

    def __itruediv__(self, other: Any) -> Self:
        self.__proxy_wrapped__ = operator.itruediv(
            self.__proxy_wrapped__,
            other,
        )
        return self

    def __ifloordiv__(self, other: Any) -> Self:
        self.__proxy_wrapped__ //= other
        return self

    def __imod__(self, other: Any) -> Self:
        self.__proxy_wrapped__ %= other
        return self

    def __ipow__(self, other: Any) -> Self:  # type: ignore[misc]
        self.__proxy_wrapped__ **= other
        return self

    def __ilshift__(self, other: Any) -> Self:
        self.__proxy_wrapped__ <<= other
        return self

    def __irshift__(self, other: Any) -> Self:
        self.__proxy_wrapped__ >>= other
        return self

    def __iand__(self, other: Any) -> Self:
        self.__proxy_wrapped__ &= other
        return self

    def __ixor__(self, other: Any) -> Self:
        self.__proxy_wrapped__ ^= other
        return self

    def __ior__(self, other: Any) -> Self:
        self.__proxy_wrapped__ |= other
        return self

    def __neg__(self) -> Any:
        return -self.__proxy_wrapped__  # type: ignore[operator]

    def __pos__(self) -> Any:
        return +self.__proxy_wrapped__  # type: ignore[operator]

    def __abs__(self) -> Any:
        return abs(self.__proxy_wrapped__)  # type: ignore[arg-type]

    def __invert__(self) -> Any:
        return ~self.__proxy_wrapped__  # type: ignore[operator]

    def __int__(self) -> int:
        return int(self.__proxy_wrapped__)  # type: ignore[call-overload]

    def __float__(self) -> float:
        return float(self.__proxy_wrapped__)  # type: ignore[arg-type]

    def __index__(self) -> int:
        if hasattr(self.__proxy_wrapped__, '__index__'):
            return operator.index(self.__proxy_wrapped__)
        else:
            return int(self.__proxy_wrapped__)  # type: ignore[call-overload]

    def __len__(self) -> int:
        return len(self.__proxy_wrapped__)  # type: ignore[arg-type]

    def __contains__(self, value: Any) -> bool:
        return value in self.__proxy_wrapped__  # type: ignore[operator]

    def __getitem__(self, key: Any) -> Any:
        return self.__proxy_wrapped__[key]  # type: ignore[index]

    def __setitem__(self, key: Any, value: Any) -> None:
        self.__proxy_wrapped__[key] = value  # type: ignore[index]

    def __delitem__(self, key: Any) -> None:
        del self.__proxy_wrapped__[key]  # type: ignore[attr-defined]

    def __enter__(self) -> Any:
        return self.__proxy_wrapped__.__enter__()  # type: ignore[attr-defined]

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        return self.__proxy_wrapped__.__exit__(*args, **kwargs)  # type: ignore[attr-defined]

    def __iter__(self) -> Iterator[Any]:
        return iter(self.__proxy_wrapped__)  # type: ignore[call-overload]

    def __next__(self) -> Any:
        return next(self.__proxy_wrapped__)  # type: ignore[call-overload]

    def __call__(self, *args: Any, **kwargs: Any) -> Any:  # noqa: D102
        return self.__proxy_wrapped__(*args, **kwargs)  # type: ignore[operator]

    def __reduce__(
        self,
    ) -> tuple[
        Callable[
            [FactoryType[T], DefaultClassType, DefaultHashType],
            Proxy[T],
        ],
        tuple[FactoryType[T], DefaultClassType, DefaultHashType],
    ]:
        factory = object.__getattribute__(self, '__proxy_factory__')
        default_class = object.__getattribute__(
            self,
            '__proxy_default_class__',
        )
        default_hash = object.__getattribute__(self, '__proxy_default_hash__')
        return _proxy_trampoline, (factory, default_class, default_hash)

    def __reduce_ex__(
        self,
        protocol: SupportsIndex,
    ) -> tuple[
        Callable[
            [FactoryType[T], DefaultClassType, DefaultHashType],
            Proxy[T],
        ],
        tuple[FactoryType[T], DefaultClassType, DefaultHashType],
    ]:
        return self.__reduce__()

    def __aiter__(self) -> Any:
        return self.__proxy_wrapped__.__aiter__()  # type: ignore[attr-defined]

    async def __anext__(self) -> Any:  # pragma: no cover
        return await self.__proxy_wrapped__.__anext__()  # type: ignore[attr-defined]

    def __await__(self) -> Any:  # pragma: no cover
        obj_type = type(self.__proxy_wrapped__)
        if (
            obj_type is CoroutineType
            or (
                obj_type is GeneratorType
                and bool(
                    self.__proxy_wrapped__.gi_code.co_flags  # type: ignore[attr-defined]
                    & CO_ITERABLE_COROUTINE,
                )
            )
            or isinstance(self.__proxy_wrapped__, Awaitable)
        ):
            return _do_await(self.__proxy_wrapped__).__await__()
        else:
            return _do_yield_from(self.__proxy_wrapped__)

    def __aenter__(self) -> Any:
        return self.__proxy_wrapped__.__aenter__()  # type: ignore[attr-defined]

    def __aexit__(self, *args: Any, **kwargs: Any) -> Any:
        return self.__proxy_wrapped__.__aexit__(*args, **kwargs)  # type: ignore[attr-defined]


_ProxyOr: TypeAlias = Union[Proxy[T], T]
ProxyOr = TypeAliasType('ProxyOr', Union[Proxy[T], T], type_params=(T,))
"""Type alias for a union of a type `T` or a `Proxy[T]`.

This type alias is useful for typing functions that operate on or
return mixed types involving proxies.

Example:
    ```python
    from typing import TypeVar
    from proxystore.proxy import Proxy, ProxyOr, extract

    T = TypeVar('T')

    def extract_if_proxy(value: ProxyOr[T]) -> T:
        return extract(value) if isinstance(value, Proxy) else value
    ```
"""


def get_factory(proxy: Proxy[T]) -> FactoryType[T]:
    """Get the factory contained in a proxy.

    Args:
        proxy: Proxy instance to get the factory from.

    Returns:
        The factory, a callable object which, when invoked, returns an object
        of type `T`.
    """
    return proxy.__proxy_factory__


def extract(proxy: Proxy[T]) -> T:
    """Return object wrapped by proxy.

    If the proxy has not been resolved yet, this will force
    the proxy to be resolved prior.

    Args:
        proxy: Proxy instance to extract from.

    Returns:
        Object wrapped by proxy.
    """
    return proxy.__proxy_wrapped__


def is_resolved(proxy: Proxy[T]) -> bool:
    """Check if a proxy is resolved.

    Args:
        proxy: Proxy instance to check.

    Returns:
        `True` if `proxy` is resolved (i.e., the `factory` has been called) \
        and `False` otherwise.
    """
    return proxy.__proxy_resolved__


def resolve(proxy: Proxy[T]) -> None:
    """Force a proxy to resolve itself.

    Args:
        proxy: Proxy instance to force resolve.
    """
    proxy.__proxy_wrapped__  # noqa: B018


class ProxyLocker(Generic[T]):
    """Proxy locker that prevents resolution of wrapped proxies.

    The class prevents unintended access to a wrapped proxy to ensure a proxy
    is not resolved. The wrapped proxy can be retrieved with
    `#!python proxy = ProxyLocker(proxy).unlock()`.

    Args:
        proxy: Proxy to lock.
    """

    def __init__(self, proxy: Proxy[T]) -> None:
        self._proxy = proxy

    def __getattribute__(self, attr: str) -> Any:
        # Override to raise an error if the proxy is accessed.
        if attr == '_proxy':
            raise AttributeError('Cannot access proxy attribute of a Locker')
        return super().__getattribute__(attr)

    def unlock(self) -> Proxy[T]:
        """Retrieve the locked proxy.

        Returns:
            Proxy object.
        """
        return super().__getattribute__('_proxy')
