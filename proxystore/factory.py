"""ProxyStore Factory Implementations.

Factories are callable classes that wrap up the functionality needed
to resolve a proxy, where resolving is the process of retrieving the
object from wherever it is stored such that the proxy can act as the
object.
"""
from __future__ import annotations

from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Callable
from typing import Generic
from typing import TypeVar

_default_pool = ThreadPoolExecutor()

T = TypeVar('T')


class Factory(Generic[T]):
    """Abstract Factory Class.

    A factory is a callable object that when called, returns an object.
    The :any:`Proxy <proxystore.proxy.Proxy>` constructor takes an instance of
    a factory and calls the factory when the proxy does its just-in-time
    resolution.

    Note:
        All factory implementations must be subclasses of
        :class:`Factory <.Factory>`.

    Note:
        If a custom factory is not-pickleable,
        :func:`__getnewargs_ex__` may need to be implemented.
        Writing custom pickling functions is also beneifical to ensure that
        a pickled factory does not contain the object itself, just what is
        needed to resolve the object to keep the final pickled factory as
        small as possible.
    """

    def __init__(self) -> None:
        """Init Factory."""
        raise NotImplementedError

    def __call__(self) -> T:
        """Aliases :func:`resolve()`."""
        return self.resolve()

    def resolve(self) -> T:
        """Resolve and return object."""
        raise NotImplementedError

    def resolve_async(self) -> None:
        """Asynchronously resolve object.

        Note:
            The API has no requirements about the implementation
            details of this method, only that :func:`resolve()` will
            correctly deal with any side-effects of a call to
            :func:`resolve_async()`.
        """
        pass


class SimpleFactory(Factory[T]):
    """Simple Factory that stores object as class attribute."""

    def __init__(self, obj: T) -> None:
        """Init Factory.

        Args:
            obj (object): object to produce when factory is called.
        """
        self._obj = obj

    def __call__(self) -> T:
        """Resolve object."""
        return self.resolve()

    def resolve(self) -> T:
        """Return object."""
        return self._obj


class LambdaFactory(Factory[T]):
    """Factory that takes any callable object."""

    def __init__(
        self,
        target: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Init LambdaFactory.

        Args:
            target (callable): callable object (function, class, lambda) to be
                invoked when the factory is resolved.
            args (tuple): argument tuple for target invocation (default: ()).
            kwargs (dict): dictionary of keyword arguments for target
                invocation (default: {}).
        """
        self._target = target
        self._args = args
        self._kwargs = kwargs
        self._obj_future: Future[T] | None = None

    def resolve(self) -> T:
        """Return target object."""
        if self._obj_future is not None:
            obj = self._obj_future.result()
            self._obj_future = None
            return obj

        return self._target(*self._args, **self._kwargs)

    def resolve_async(self) -> None:
        """Asynchronously retrieve target object.

        A subsequent call to :func:`resolve` will wait on the future and
        return the result.
        """
        self._obj_future = _default_pool.submit(
            self._target,
            *self._args,
            **self._kwargs,
        )
