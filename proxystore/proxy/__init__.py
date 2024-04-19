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
from inspect import CO_ITERABLE_COROUTINE
from types import CoroutineType
from types import GeneratorType
from typing import Any
from typing import Callable
from typing import cast
from typing import Generic
from typing import Iterator
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

from proxystore.proxy._utils import _do_await
from proxystore.proxy._utils import _do_yield_from
from proxystore.proxy._utils import as_metaclass
from proxystore.proxy._utils import ProxyMetaType

T = TypeVar('T')
FactoryType: TypeAlias = Callable[[], T]


class Proxy(as_metaclass(ProxyMetaType), Generic[T]):  # type: ignore[misc]
    """Lazy object proxy.

    An extension of the Proxy from
    [lazy-object-proxy](https://github.com/ionelmc/python-lazy-object-proxy){target=_blank}
    with modified attribute lookup and pickling behavior.

    An object proxy acts as a thin wrapper around a Python object, i.e.
    the proxy behaves identically to the underlying object. The proxy is
    initialized with a callable factory object. The factory returns the
    underlying object when called, i.e. 'resolves' the proxy. The does
    just-in-time resolution, i.e., the proxy
    does not call the factory until the first access to the proxy (hence, the
    lazy aspect of the proxy).

    The factory contains the mechanisms to appropriately resolve the object,
    e.g., which in the case for ProxyStore means requesting the correct
    object from the backend store.

    ```python
    x = np.array([1, 2, 3])
    f = ps.factory.SimpleFactory(x)
    p = ps.proxy.Proxy(f)
    assert isinstance(p, np.ndarray)
    assert np.array_equal(p, [1, 2, 3])
    ```

    Note:
        The `factory`, by default, is only ever called once during the
        lifetime of a proxy instance.

    Note:
        When a proxy instance is pickled, only the `factory` is pickled, not
        the wrapped object. Thus, proxy instances can be pickled and passed
        around cheaply, and once the proxy is unpickled and used, the `factory`
        will be called again to resolve the object.

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
        __factory__: Factory function which resolves to the target object.
        __target__: The target object once resolved.
        __resolved__: `True` if `__target__` is set.
        __wrapped__: A property that either returns `__target__` if it
            exists else calls `__factory__`, saving the result to `__target__`
            and returning said result.

    Args:
        factory: Callable object that returns the underlying object when
            called.

    Raises:
        TypeError: If `factory` is not callable.
    """

    __slots__ = '__target__', '__factory__'

    __target__: T
    #  This type annotations confuses mypy.
    # __factory__: FactoryType[T]

    def __init__(self, factory: FactoryType[T]) -> None:
        if not callable(factory):
            raise TypeError('Factory must be callable.')
        object.__setattr__(self, '__factory__', factory)

    @property
    def __resolved__(self) -> bool:
        try:
            object.__getattribute__(self, '__target__')
        except AttributeError:
            return False
        else:
            return True

    @property
    def __wrapped__(self) -> T:
        try:
            return cast(T, object.__getattribute__(self, '__target__'))
        except AttributeError:
            try:
                factory = object.__getattribute__(self, '__factory__')
            except AttributeError as exc:
                raise ValueError(
                    "Proxy hasn't been initiated: __factory__ is missing.",
                ) from exc
            target = factory()
            object.__setattr__(self, '__target__', target)
            return target

    @__wrapped__.deleter
    def __wrapped__(self) -> None:
        object.__delattr__(self, '__target__')

    @__wrapped__.setter
    def __wrapped__(self, target: T) -> None:
        object.__setattr__(self, '__target__', target)

    @property
    def __name__(self) -> str:
        return self.__wrapped__.__name__  # type: ignore[attr-defined]

    @__name__.setter
    def __name__(self, value: str) -> None:
        self.__wrapped__.__name__ = value  # type: ignore[attr-defined]

    @property
    def __class__(self) -> Any:
        return self.__wrapped__.__class__

    @__class__.setter
    def __class__(self, value: Any) -> None:  # pragma: no cover
        self.__wrapped__.__class__ = value

    def __dir__(self) -> Any:
        return dir(self.__wrapped__)

    def __str__(self) -> str:
        return str(self.__wrapped__)

    def __bytes__(self) -> bytes:
        return bytes(self.__wrapped__)  # type: ignore[call-overload]

    def __repr__(self) -> str:
        try:
            target = object.__getattribute__(self, '__target__')
        except AttributeError:
            return (
                f'<{type(self).__name__} at 0x{id(self):x} with '
                f'factory {self.__factory__!r}>'
            )
        else:
            return (
                f'<{type(self).__name__} at 0x{id(self):x} '
                f'wrapping {target!r} at 0x{id(target):x} with '
                f'factory {self.__factory__!r}>'
            )

    def __fspath__(self) -> Any:
        wrapped = self.__wrapped__
        if isinstance(wrapped, (bytes, str)):
            return wrapped
        else:
            fspath = getattr(wrapped, '__fspath__', None)
            if fspath is None:
                return wrapped
            else:
                return fspath()

    def __reversed__(self) -> Any:
        return reversed(self.__wrapped__)  # type: ignore[call-overload]

    def __round__(self) -> Any:
        return round(self.__wrapped__)  # type: ignore[call-overload]

    def __lt__(self, other: Any) -> bool:
        return self.__wrapped__ < other

    def __le__(self, other: Any) -> bool:
        return self.__wrapped__ <= other

    def __eq__(self, other: Any) -> bool:
        return self.__wrapped__ == other

    def __ne__(self, other: Any) -> bool:
        return self.__wrapped__ != other

    def __gt__(self, other: Any) -> bool:
        return self.__wrapped__ > other

    def __ge__(self, other: Any) -> bool:
        return self.__wrapped__ >= other

    def __hash__(self) -> int:
        return hash(self.__wrapped__)

    def __bool__(self) -> bool:
        return bool(self.__wrapped__)

    def __setattr__(self, name: str, value: Any) -> None:
        if hasattr(type(self), name):
            object.__setattr__(self, name, value)
        else:
            setattr(self.__wrapped__, name, value)

    def __getattr__(self, name: str) -> Any:
        if name in ('__wrapped__', '__factory__'):
            raise AttributeError(name)
        else:
            return getattr(self.__wrapped__, name)

    def __delattr__(self, name: str) -> None:
        if hasattr(type(self), name):
            object.__delattr__(self, name)
        else:
            delattr(self.__wrapped__, name)

    def __add__(self, other: Any) -> Any:
        return self.__wrapped__ + other

    def __sub__(self, other: Any) -> Any:
        return self.__wrapped__ - other

    def __mul__(self, other: Any) -> Any:
        return self.__wrapped__ * other

    def __matmul__(self, other: Any) -> Any:
        return self.__wrapped__ @ other

    def __truediv__(self, other: Any) -> Any:
        return operator.truediv(self.__wrapped__, other)

    def __floordiv__(self, other: Any) -> Any:
        return self.__wrapped__ // other

    def __mod__(self, other: Any) -> Any:
        return self.__wrapped__ % other

    def __divmod__(self, other: Any) -> Any:
        return divmod(self.__wrapped__, other)

    def __pow__(self, other: Any, *args: Any) -> Any:
        return pow(self.__wrapped__, other, *args)  # type: ignore[call-overload]

    def __lshift__(self, other: Any) -> Any:
        return self.__wrapped__ << other

    def __rshift__(self, other: Any) -> Any:
        return self.__wrapped__ >> other

    def __and__(self, other: Any) -> Any:
        return self.__wrapped__ & other

    def __xor__(self, other: Any) -> Any:
        return self.__wrapped__ ^ other

    def __or__(self, other: Any) -> Any:
        return self.__wrapped__ | other

    def __radd__(self, other: Any) -> Any:
        return other + self.__wrapped__

    def __rsub__(self, other: Any) -> Any:
        return other - self.__wrapped__

    def __rmul__(self, other: Any) -> Any:
        return other * self.__wrapped__

    def __rmatmul__(self, other: Any) -> Any:
        return other @ self.__wrapped__

    def __rtruediv__(self, other: Any) -> Any:
        return operator.truediv(other, self.__wrapped__)

    def __rfloordiv__(self, other: Any) -> Any:
        return other // self.__wrapped__

    def __rmod__(self, other: Any) -> Any:
        return other % self.__wrapped__

    def __rdivmod__(self, other: Any) -> Any:
        return divmod(other, self.__wrapped__)

    def __rpow__(self, other: Any, *args: Any) -> Any:
        return pow(other, self.__wrapped__, *args)

    def __rlshift__(self, other: Any) -> Any:
        return other << self.__wrapped__

    def __rrshift__(self, other: Any) -> Any:
        return other >> self.__wrapped__

    def __rand__(self, other: Any) -> Any:
        return other & self.__wrapped__

    def __rxor__(self, other: Any) -> Any:
        return other ^ self.__wrapped__

    def __ror__(self, other: Any) -> Any:
        return other | self.__wrapped__

    def __iadd__(self, other: Any) -> Self:
        self.__wrapped__ += other
        return self

    def __isub__(self, other: Any) -> Self:
        self.__wrapped__ -= other
        return self

    def __imul__(self, other: Any) -> Self:
        self.__wrapped__ *= other
        return self

    def __imatmul__(self, other: Any) -> Self:
        self.__wrapped__ @= other
        return self

    def __itruediv__(self, other: Any) -> Self:
        self.__wrapped__ = operator.itruediv(self.__wrapped__, other)
        return self

    def __ifloordiv__(self, other: Any) -> Self:
        self.__wrapped__ //= other
        return self

    def __imod__(self, other: Any) -> Self:
        self.__wrapped__ %= other
        return self

    def __ipow__(self, other: Any) -> Self:  # type: ignore[misc]
        self.__wrapped__ **= other
        return self

    def __ilshift__(self, other: Any) -> Self:
        self.__wrapped__ <<= other
        return self

    def __irshift__(self, other: Any) -> Self:
        self.__wrapped__ >>= other
        return self

    def __iand__(self, other: Any) -> Self:
        self.__wrapped__ &= other
        return self

    def __ixor__(self, other: Any) -> Self:
        self.__wrapped__ ^= other
        return self

    def __ior__(self, other: Any) -> Self:
        self.__wrapped__ |= other
        return self

    def __neg__(self) -> Any:
        return -self.__wrapped__  # type: ignore[operator]

    def __pos__(self) -> Any:
        return +self.__wrapped__  # type: ignore[operator]

    def __abs__(self) -> Any:
        return abs(self.__wrapped__)  # type: ignore[arg-type]

    def __invert__(self) -> Any:
        return ~self.__wrapped__  # type: ignore[operator]

    def __int__(self) -> int:
        return int(self.__wrapped__)  # type: ignore[call-overload]

    def __float__(self) -> float:
        return float(self.__wrapped__)  # type: ignore[arg-type]

    def __index__(self) -> int:
        if hasattr(self.__wrapped__, '__index__'):
            return operator.index(self.__wrapped__)
        else:
            return int(self.__wrapped__)  # type: ignore[call-overload]

    def __len__(self) -> int:
        return len(self.__wrapped__)  # type: ignore[arg-type]

    def __contains__(self, value: Any) -> bool:
        return value in self.__wrapped__  # type: ignore[operator]

    def __getitem__(self, key: Any) -> Any:
        return self.__wrapped__[key]  # type: ignore[index]

    def __setitem__(self, key: Any, value: Any) -> None:
        self.__wrapped__[key] = value  # type: ignore[index]

    def __delitem__(self, key: Any) -> None:
        del self.__wrapped__[key]  # type: ignore[attr-defined]

    def __enter__(self) -> Any:
        return self.__wrapped__.__enter__()  # type: ignore[attr-defined]

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        return self.__wrapped__.__exit__(*args, **kwargs)  # type: ignore[attr-defined]

    def __iter__(self) -> Iterator[Any]:
        return iter(self.__wrapped__)  # type: ignore[call-overload]

    def __next__(self) -> Any:
        return next(self.__wrapped__)  # type: ignore[call-overload]

    def __call__(self, *args: Any, **kwargs: Any) -> Any:  # noqa: D102
        return self.__wrapped__(*args, **kwargs)  # type: ignore[operator]

    def __reduce__(
        self,
    ) -> tuple[Callable[[FactoryType[T]], Proxy[T]], tuple[FactoryType[T]]]:
        return Proxy, (object.__getattribute__(self, '__factory__'),)

    def __reduce_ex__(
        self,
        protocol: SupportsIndex,
    ) -> tuple[Callable[[FactoryType[T]], Proxy[T]], tuple[FactoryType[T]]]:
        return self.__reduce__()

    def __aiter__(self) -> Any:
        return self.__wrapped__.__aiter__()  # type: ignore[attr-defined]

    async def __anext__(self) -> Any:  # pragma: no cover
        return await self.__wrapped__.__anext__()  # type: ignore[attr-defined]

    def __await__(self) -> Any:  # pragma: no cover
        obj_type = type(self.__wrapped__)
        if (
            obj_type is CoroutineType
            or obj_type is GeneratorType
            and bool(
                self.__wrapped__.gi_code.co_flags  # type: ignore[attr-defined]
                & CO_ITERABLE_COROUTINE,
            )
            or isinstance(self.__wrapped__, Awaitable)
        ):
            return _do_await(self.__wrapped__).__await__()
        else:
            return _do_yield_from(self.__wrapped__)

    def __aenter__(self) -> Any:
        return self.__wrapped__.__aenter__()  # type: ignore[attr-defined]

    def __aexit__(self, *args: Any, **kwargs: Any) -> Any:
        return self.__wrapped__.__aexit__(*args, **kwargs)  # type: ignore[attr-defined]


ProxyType: TypeAlias = Union[Proxy[T], T]


def extract(proxy: Proxy[T]) -> T:
    """Return object wrapped by proxy.

    If the proxy has not been resolved yet, this will force
    the proxy to be resolved prior.

    Args:
        proxy: Proxy instance to extract from.

    Returns:
        Object wrapped by proxy.
    """
    return proxy.__wrapped__


def is_resolved(proxy: Proxy[T]) -> bool:
    """Check if a proxy is resolved.

    Args:
        proxy: Proxy instance to check.

    Returns:
        `True` if `proxy` is resolved (i.e., the `factory` has been called) \
        and `False` otherwise.
    """
    return proxy.__resolved__


def resolve(proxy: Proxy[T]) -> None:
    """Force a proxy to resolve itself.

    Args:
        proxy: Proxy instance to force resolve.
    """
    proxy.__wrapped__  # noqa: B018


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
