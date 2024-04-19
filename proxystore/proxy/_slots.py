# This module contains source code from python-lazy-object-proxy v1.10.0
# which is available under the BSD 2-Clause License included below.
#
# The following modifications to the source has been made:
#   * Replaced certain uses of @property with a custom @proxy_property.
#   * Consolidated source from multiple modules into this single module.
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

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from proxystore.proxy._property import proxy_property

T = TypeVar('T')


async def _do_await(obj):  # type: ignore[no-untyped-def] # pragma: no cover
    return await obj


def _do_yield_from(gen):  # type: ignore[no-untyped-def] # pragma: no cover
    return (yield from gen)


def _identity(obj: T) -> T:
    return obj


def _with_proxy_metaclass(meta: Any, *bases: Any) -> Any:
    return meta('ProxyMetaClass', bases, {})


class _ProxyMethods:
    # We use properties to override the values of __module__ and
    # __doc__. If we add these in ObjectProxy, the derived class
    # __dict__ will still be setup to have string variants of these
    # attributes and the rules of descriptors means that they appear to
    # take precedence over the properties in the base class. To avoid
    # that, we copy the properties into the derived class type itself
    # via a meta class. In that way the properties will always take
    # precedence.
    __wrapped__: Any

    @proxy_property(default='proxystore.proxy')
    def __module__(self) -> str:  # type: ignore[override]
        return self.__wrapped__.__module__

    @__module__.setter
    def __module__set(self, value: str) -> None:
        self.__wrapped__.__module__ = value

    @proxy_property(default='<Proxy Placeholder Docstring>')
    def __doc__(self) -> str:  # type: ignore[override]
        return self.__wrapped__.__doc__

    @__doc__.setter
    def __doc__set(self, value: str) -> None:
        self.__wrapped__.__doc__ = value

    @property
    def __annotations__(self) -> Any:
        return self.__wrapped__.__annotations__

    @__annotations__.setter
    def __annotations__(self, value: Any) -> None:
        self.__wrapped__.__annotations__ = value

    # We similar use a property for __dict__. We need __dict__ to be
    # explicit to ensure that vars() works as expected.

    @property
    def __dict__(self) -> Any:  # type: ignore[override]
        return self.__wrapped__.__dict__

    # Need to also propagate the special __weakref__ attribute for case
    # where decorating classes which will define this. If do not define
    # it and use a function like inspect.getmembers() on a decorator
    # class it will fail. This can't be in the derived classes.

    @property
    def __weakref__(self) -> Any:
        return self.__wrapped__.__weakref__


class _ProxyMetaType(type):
    def __new__(
        cls,
        name: str,
        bases: tuple[Any, ...],
        dictionary: dict[Any, Any],
    ) -> Any:
        # Copy our special properties into the class so that they
        # always take precedence over attributes of the same name added
        # during construction of a derived class. This is to save
        # duplicating the implementation for them in all derived classes.

        dictionary.update(vars(_ProxyMethods))
        # dictionary.pop('__dict__')

        return type.__new__(cls, name, bases, dictionary)


class SlotsProxy(_with_proxy_metaclass(_ProxyMetaType), Generic[T]):  # type: ignore[misc]
    """A proxy implementation in pure Python, using slots.

    You can subclass this to add local methods or attributes, or enable
    __dict__.

    The most important internals:

    * `__factory__` is the callback that "materializes" the object we proxy to.
    * `__target__` will contain the object we proxy to, once it's
      "materialized".
    * `__resolved__` is a boolean, `True` if factory was called.
    * `__wrapped__` is a property that does either:

      * return `__target__` if it's set.
      * calls `__factory__`, saves result to `__target__` and returns said
        result.
    """

    __slots__ = '__target__', '__factory__'

    def __init__(self, factory: Callable[[], T]) -> None:
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

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.__wrapped__(*args, **kwargs)  # type: ignore[operator]

    def __reduce__(self) -> tuple[Any, Any]:  # pragma: no cover
        return _identity, (self.__wrapped__,)

    def __reduce_ex__(self, protocol: SupportsIndex) -> tuple[Any, Any]:
        return _identity, (self.__wrapped__,)

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
