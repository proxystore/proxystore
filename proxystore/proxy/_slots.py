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

# mypy: ignore-errors
from __future__ import annotations

import operator
from collections.abc import Awaitable
from inspect import CO_ITERABLE_COROUTINE
from types import CoroutineType
from types import GeneratorType

from proxystore.proxy._property import proxy_property


async def do_await(obj):
    return await obj


def do_yield_from(gen):
    return (yield from gen)


def await_(obj):
    obj_type = type(obj)
    if (
        obj_type is CoroutineType
        or obj_type is GeneratorType
        and bool(obj.gi_code.co_flags & CO_ITERABLE_COROUTINE)
        or isinstance(obj, Awaitable)
    ):
        return do_await(obj).__await__()
    else:
        return do_yield_from(obj)


def __aiter__(self):  # noqa: N807
    return self.__wrapped__.__aiter__()


async def __anext__(self):  # noqa: N807
    return await self.__wrapped__.__anext__()


def __await__(self):  # noqa: N807
    return await_(self.__wrapped__)


def __aenter__(self):  # noqa: N807
    return self.__wrapped__.__aenter__()


def __aexit__(self, *args, **kwargs):  # noqa: N807
    return self.__wrapped__.__aexit__(*args, **kwargs)


def identity(obj):
    return obj


def make_proxy_method(code):
    def proxy_wrapper(self, *args):
        return code(self.__wrapped__, *args)

    return proxy_wrapper


def _with_proxy_metaclass(meta, *bases):
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

    @proxy_property(default='proxystore.proxy')
    def __module__(self) -> str:
        return self.__wrapped__.__module__

    @__module__.setter
    def __module__set(self, value: str) -> None:
        self.__wrapped__.__module__ = value

    @proxy_property(default='<Proxy Placeholder Docstring>')
    def __doc__(self) -> str:
        return self.__wrapped__.__doc__

    @__doc__.setter
    def __doc__set(self, value: str) -> None:
        self.__wrapped__.__doc__ = value

    # We similar use a property for __dict__. We need __dict__ to be
    # explicit to ensure that vars() works as expected.

    @property
    def __dict__(self):
        return self.__wrapped__.__dict__

    # Need to also propagate the special __weakref__ attribute for case
    # where decorating classes which will define this. If do not define
    # it and use a function like inspect.getmembers() on a decorator
    # class it will fail. This can't be in the derived classes.

    @property
    def __weakref__(self):
        return self.__wrapped__.__weakref__


class _ProxyMetaType(type):
    def __new__(cls, name, bases, dictionary):
        # Copy our special properties into the class so that they
        # always take precedence over attributes of the same name added
        # during construction of a derived class. This is to save
        # duplicating the implementation for them in all derived classes.

        dictionary.update(vars(_ProxyMethods))
        # dictionary.pop('__dict__')

        return type.__new__(cls, name, bases, dictionary)


class SlotsProxy(_with_proxy_metaclass(_ProxyMetaType)):
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

    def __init__(self, factory):
        object.__setattr__(self, '__factory__', factory)

    @property
    def __resolved__(self, __getattr__=object.__getattribute__):
        try:
            __getattr__(self, '__target__')
        except AttributeError:
            return False
        else:
            return True

    @property
    def __wrapped__(
        self,
        __getattr__=object.__getattribute__,
        __setattr__=object.__setattr__,
        __delattr__=object.__delattr__,
    ):
        try:
            return __getattr__(self, '__target__')
        except AttributeError:
            try:
                factory = __getattr__(self, '__factory__')
            except AttributeError as exc:
                raise ValueError(
                    "Proxy hasn't been initiated: __factory__ is missing.",
                ) from exc
            target = factory()
            __setattr__(self, '__target__', target)
            return target

    @__wrapped__.deleter
    def __wrapped__(self, __delattr__=object.__delattr__):
        __delattr__(self, '__target__')

    @__wrapped__.setter
    def __wrapped__(self, target, __setattr__=object.__setattr__):
        __setattr__(self, '__target__', target)

    @property
    def __name__(self):
        return self.__wrapped__.__name__

    @__name__.setter
    def __name__(self, value):
        self.__wrapped__.__name__ = value

    @property
    def __class__(self):
        return self.__wrapped__.__class__

    @__class__.setter
    def __class__(self, value):
        self.__wrapped__.__class__ = value

    @property
    def __annotations__(self):
        return self.__wrapped__.__anotations__

    @__annotations__.setter
    def __annotations__(self, value):
        self.__wrapped__.__annotations__ = value

    def __dir__(self):
        return dir(self.__wrapped__)

    def __str__(self):
        return str(self.__wrapped__)

    def __bytes__(self):
        return bytes(self.__wrapped__)

    def __repr__(self, __getattr__=object.__getattribute__):
        try:
            target = __getattr__(self, '__target__')
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

    def __fspath__(self):
        wrapped = self.__wrapped__
        if isinstance(wrapped, (bytes, str)):
            return wrapped
        else:
            fspath = getattr(wrapped, '__fspath__', None)
            if fspath is None:
                return wrapped
            else:
                return fspath()

    def __reversed__(self):
        return reversed(self.__wrapped__)

    def __round__(self):
        return round(self.__wrapped__)

    def __lt__(self, other):
        return self.__wrapped__ < other

    def __le__(self, other):
        return self.__wrapped__ <= other

    def __eq__(self, other):
        return self.__wrapped__ == other

    def __ne__(self, other):
        return self.__wrapped__ != other

    def __gt__(self, other):
        return self.__wrapped__ > other

    def __ge__(self, other):
        return self.__wrapped__ >= other

    def __hash__(self):
        return hash(self.__wrapped__)

    def __nonzero__(self):
        return bool(self.__wrapped__)

    def __bool__(self):
        return bool(self.__wrapped__)

    def __setattr__(self, name, value, __setattr__=object.__setattr__):
        if hasattr(type(self), name):
            __setattr__(self, name, value)
        else:
            setattr(self.__wrapped__, name, value)

    def __getattr__(self, name):
        if name in ('__wrapped__', '__factory__'):
            raise AttributeError(name)
        else:
            return getattr(self.__wrapped__, name)

    def __delattr__(self, name, __delattr__=object.__delattr__):
        if hasattr(type(self), name):
            __delattr__(self, name)
        else:
            delattr(self.__wrapped__, name)

    def __add__(self, other):
        return self.__wrapped__ + other

    def __sub__(self, other):
        return self.__wrapped__ - other

    def __mul__(self, other):
        return self.__wrapped__ * other

    def __matmul__(self, other):
        return self.__wrapped__ @ other

    def __truediv__(self, other):
        return operator.truediv(self.__wrapped__, other)

    def __floordiv__(self, other):
        return self.__wrapped__ // other

    def __mod__(self, other):
        return self.__wrapped__ % other

    def __divmod__(self, other):
        return divmod(self.__wrapped__, other)

    def __pow__(self, other, *args):
        return pow(self.__wrapped__, other, *args)

    def __lshift__(self, other):
        return self.__wrapped__ << other

    def __rshift__(self, other):
        return self.__wrapped__ >> other

    def __and__(self, other):
        return self.__wrapped__ & other

    def __xor__(self, other):
        return self.__wrapped__ ^ other

    def __or__(self, other):
        return self.__wrapped__ | other

    def __radd__(self, other):
        return other + self.__wrapped__

    def __rsub__(self, other):
        return other - self.__wrapped__

    def __rmul__(self, other):
        return other * self.__wrapped__

    def __rmatmul__(self, other):
        return other @ self.__wrapped__

    def __rdiv__(self, other):
        return operator.div(other, self.__wrapped__)

    def __rtruediv__(self, other):
        return operator.truediv(other, self.__wrapped__)

    def __rfloordiv__(self, other):
        return other // self.__wrapped__

    def __rmod__(self, other):
        return other % self.__wrapped__

    def __rdivmod__(self, other):
        return divmod(other, self.__wrapped__)

    def __rpow__(self, other, *args):
        return pow(other, self.__wrapped__, *args)

    def __rlshift__(self, other):
        return other << self.__wrapped__

    def __rrshift__(self, other):
        return other >> self.__wrapped__

    def __rand__(self, other):
        return other & self.__wrapped__

    def __rxor__(self, other):
        return other ^ self.__wrapped__

    def __ror__(self, other):
        return other | self.__wrapped__

    def __iadd__(self, other):
        self.__wrapped__ += other
        return self

    def __isub__(self, other):
        self.__wrapped__ -= other
        return self

    def __imul__(self, other):
        self.__wrapped__ *= other
        return self

    def __imatmul__(self, other):
        self.__wrapped__ @= other
        return self

    def __itruediv__(self, other):
        self.__wrapped__ = operator.itruediv(self.__wrapped__, other)
        return self

    def __ifloordiv__(self, other):
        self.__wrapped__ //= other
        return self

    def __imod__(self, other):
        self.__wrapped__ %= other
        return self

    def __ipow__(self, other):
        self.__wrapped__ **= other
        return self

    def __ilshift__(self, other):
        self.__wrapped__ <<= other
        return self

    def __irshift__(self, other):
        self.__wrapped__ >>= other
        return self

    def __iand__(self, other):
        self.__wrapped__ &= other
        return self

    def __ixor__(self, other):
        self.__wrapped__ ^= other
        return self

    def __ior__(self, other):
        self.__wrapped__ |= other
        return self

    def __neg__(self):
        return -self.__wrapped__

    def __pos__(self):
        return +self.__wrapped__

    def __abs__(self):
        return abs(self.__wrapped__)

    def __invert__(self):
        return ~self.__wrapped__

    def __int__(self):
        return int(self.__wrapped__)

    def __float__(self):
        return float(self.__wrapped__)

    def __oct__(self):
        return oct(self.__wrapped__)

    def __hex__(self):
        return hex(self.__wrapped__)

    def __index__(self):
        if hasattr(self.__wrapped__, '__index__'):
            return operator.index(self.__wrapped__)
        else:
            return int(self.__wrapped__)

    def __len__(self):
        return len(self.__wrapped__)

    def __contains__(self, value):
        return value in self.__wrapped__

    def __getitem__(self, key):
        return self.__wrapped__[key]

    def __setitem__(self, key, value):
        self.__wrapped__[key] = value

    def __delitem__(self, key):
        del self.__wrapped__[key]

    def __getslice__(self, i, j):
        return self.__wrapped__[i:j]

    def __setslice__(self, i, j, value):
        self.__wrapped__[i:j] = value

    def __delslice__(self, i, j):
        del self.__wrapped__[i:j]

    def __enter__(self):
        return self.__wrapped__.__enter__()

    def __exit__(self, *args, **kwargs):
        return self.__wrapped__.__exit__(*args, **kwargs)

    def __iter__(self):
        return iter(self.__wrapped__)

    def __next__(self):
        return next(self.__wrapped__)

    def __call__(self, *args, **kwargs):
        return self.__wrapped__(*args, **kwargs)

    def __reduce__(self):
        return identity, (self.__wrapped__,)

    def __reduce_ex__(self, protocol):
        return identity, (self.__wrapped__,)

    if await_:
        __aiter__  # noqa: B018
        __anext__  # noqa: B018
        __await__  # noqa: B018
        __aenter__  # noqa: B018
        __aexit__  # noqa: B018
