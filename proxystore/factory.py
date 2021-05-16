"""ProxyStore Factory Implementations

Factories are callable classes that wrap up the functionality needed
to resolve a proxy, where resolving is the process of retrieving the
object from wherever it is stored such that the proxy can act as the
object.
"""
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

_default_pool = ThreadPoolExecutor()


class Factory:
    """Abstract Factory Class

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
        """Init Factory"""
        raise NotImplementedError

    def __call__(self) -> Any:
        """Aliases :func:`resolve()`"""
        return self.resolve()

    def resolve(self) -> Any:
        """Resolve and return object"""
        raise NotImplementedError

    def resolve_async(self) -> None:
        """Asynchronously resolve object

        Note:
            The API has no requirements about the implementation
            details of this method, only that :func:`resolve()` will
            correctly deal with any side-effects of a call to
            :func:`resolve_async()`.
        """
        pass


class SimpleFactory(Factory):
    """Simple Factory that stores object as class attribute"""

    def __init__(self, obj: Any) -> None:
        """Init Factory

        Args:
            obj (object): object to produce when factory is called.
        """
        self._obj = obj

    def __call__(self) -> Any:
        """Resolve object"""
        return self.resolve()

    def resolve(self) -> Any:
        """Return object"""
        return self._obj

    def resolve_async(self) -> None:
        """No-op"""
        pass


class LambdaFactory(Factory):
    """Factory that takes any callable object"""

    def __init__(self, func: Callable) -> None:
        """Init LambdaFactory

        Args:
            func (callable): callable object (function, class, lambda) that
                when called produces an object.
        """
        self._func = func
        self._obj_future = None

    def resolve(self) -> Any:
        """Calls `func` and returns result"""
        if self._obj_future is not None:
            obj = self._obj_future.result()
            self._obj_future = None
            return obj

        return self._func()

    def resolve_async(self) -> None:
        """Calls `func` in separate thread and save future internally

        A subsequent call to :func:`resolve` will wait on the future and
        return the result.
        """
        self._obj_future = _default_pool.submit(self._func)
