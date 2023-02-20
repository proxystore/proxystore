"""Factory implementations.

Factories are callable classes that wrap up the functionality needed
to resolve a proxy, where resolving is the process of retrieving the
object from wherever it is stored such that the proxy can act as the
object.
"""
from __future__ import annotations

from typing import Any
from typing import Callable
from typing import Generic
from typing import TypeVar

T = TypeVar('T')


class Factory(Generic[T]):
    """Abstract Factory Class.

    A factory is a callable object that when called, returns an object.
    The [`Proxy`][proxystore.proxy.Proxy] constructor takes an instance of
    a factory and calls the factory when the proxy does its just-in-time
    resolution.

    Note:
        If a custom factory is not-pickleable, `__getnewargs_ex__` may need to
        be implemented. Writing custom pickling functions is also beneifical
        to ensure that a pickled factory does not contain the object itself,
        just what is needed to resolve the object to keep the final pickled
        factory as small as possible.
    """

    def __init__(self) -> None:
        raise NotImplementedError

    def __call__(self) -> T:
        """Alias [`Factory.resolve()`][proxystore.factory.Factory.resolve]."""
        return self.resolve()

    def resolve(self) -> T:
        """Resolve and return object."""
        raise NotImplementedError


class SimpleFactory(Factory[T]):
    """Simple Factory that stores object as class attribute.

    Args:
        obj: Object to produce when factory is called.
    """

    def __init__(self, obj: T) -> None:
        self._obj = obj

    def resolve(self) -> T:
        """Return the object."""
        return self._obj


class LambdaFactory(Factory[T]):
    """Factory that takes any callable object.

    Args:
        target: Callable object (function, class, lambda) to be
            invoked when the factory is resolved.
        args: Argument tuple for target invocation.
        kwargs: Dictionary of keyword arguments for target invocation.
    """

    def __init__(
        self,
        target: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self._target = target
        self._args = args
        self._kwargs = kwargs

    def resolve(self) -> T:
        """Return the target object."""
        return self._target(*self._args, **self._kwargs)
