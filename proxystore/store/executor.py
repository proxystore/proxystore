"""Executor wrapper that automatically proxies input and output objects.

The following example wraps a
[`ProcessPoolExecutor`][concurrent.futures.ProcessPoolExecutor] to
automatically proxy certain input and output values.
Here, we create a [`Store`][proxystore.store.base.Store] using a
[`FileConnector`][proxystore.connectors.file.FileConnector].
[`StoreExecutor`][proxystore.store.executor.StoreExecutor] takes a
`should_proxy` argument which is a callable used to determine which inputs
and output values should be proxied. In this example, we use
[`ProxyType(str)`][proxystore.store.executor.ProxyType] which cause only
instances of [`str`][str] to be proxied. All other input or output types
will be ignored.

```python
from concurrent.futures import ProcessPoolExecutor

from proxystore.connectors.file import FileConnector
from proxystore.proxy import Proxy
from proxystore.store import Store
from proxystore.store.executor import StoreExecutor, ProxyType

base_executor = ProcessPoolExecutor()
store = Store('executor-example', FileConnector('./object-cache'))

def concat(base: str, *, num: int) -> str:
    return f'{base}-{num}'

with StoreExecutor(
    base_executor,
    store=store,
    should_proxy=ProxyType(str),
) as executor:
    future = executor.submit(concat, 'foobar', num=42)
    result = future.result()

    assert isinstance(result, Proxy)
    assert result == 'foobar-42'
```

The execution of `concat`, above, uses a [`str`][str] and [`int`][int] inputs
and produces a [`str`][str] output. Because we configured the
[`StoreExecutor`][proxystore.store.executor.StoreExecutor] to proxy only
[`str`][str] instances, only the [str][str] input and output were proxied.
The [`int`][int] input was not proxied.

The `should_proxy` callable passed to
[`StoreExecutor`][proxystore.store.executor.StoreExecutor] can be as
complicated as you want. For example, you could write one which checks if
an array is larger than some threshold.
"""

from __future__ import annotations

import sys
from concurrent.futures import Executor
from concurrent.futures import Future
from typing import Any
from typing import Callable
from typing import cast
from typing import Generic
from typing import Iterable
from typing import Iterator
from typing import Mapping
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

from proxystore.proxy import Proxy
from proxystore.store import get_store
from proxystore.store.base import Store
from proxystore.store.types import StoreConfig

P = ParamSpec('P')
R = TypeVar('R')


class _FunctionWrapper(Generic[P, R]):
    def __init__(
        self,
        function: Callable[P, R],
        *,
        store_config: StoreConfig,
        should_proxy: Callable[[Any], bool],
    ) -> None:
        self.function = function
        self.store_config = store_config
        self.should_proxy = should_proxy

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        result = self.function(*args, **kwargs)

        if type(result) != Proxy and self.should_proxy(result):
            store = self.get_store()
            return store.proxy(result)
        else:
            return result

    def get_store(self) -> Store[Any]:
        store = get_store(self.store_config['name'])
        if store is None:
            store = Store.from_config(self.store_config)
        return store


def _proxy_iterable(
    items: Iterable[Any],
    store: Store[Any],
    should_proxy: Callable[[Any], bool],
) -> tuple[Any, ...]:
    def _apply(item: Any) -> Any:
        if type(item) != Proxy and should_proxy(item):
            return store.proxy(item)
        else:
            return item

    return tuple(map(_apply, items))


def _proxy_mapping(
    mapping: Mapping[Any, Any],
    store: Store[Any],
    should_proxy: Callable[[Any], bool],
) -> dict[Any, Any]:
    output = {}

    for key, value in mapping.items():
        if type(value) != Proxy and should_proxy(value):
            output[key] = store.proxy(value)
        else:
            output[key] = value

    return output


class ProxyAlways:
    """Should-proxy callable which always returns `True`."""

    def __call__(self, item: Any) -> bool:
        return True


class ProxyNever:
    """Should-proxy callable which always returns `False`."""

    def __call__(self, item: Any) -> bool:
        return False


class ProxyType:
    """Proxy objects with matching types.

    Example:
        ```python
        from proxystore.store.executor import ProxyType

        should_proxy = ProxyType(float, str)
        assert not should_proxy([1, 2, 3])
        assert should_proxy(3.14)
        assert should_proxy('Hello, World!')
        ```

    Args:
        types: Variable number of object types for which objects of that
            type should be proxied.
    """

    def __init__(self, *types: type) -> None:
        self.types = types

    def __call__(self, item: Any) -> bool:
        return isinstance(item, self.types)


class StoreExecutor(Executor):
    """Executor wrapper that automatically proxies arguments and results.

    Args:
        executor: Executor to use for scheduling callables. This class
            takes ownership of `executor`, meaning that, when closed, it will
            also close `executor`.
        store: Store to use for proxying arguments and results. This class
            takes ownership of `store`, meaning that, when closed, it will
            also close `store`.
        should_proxy: Callable used to determine which arguments and results
            should be proxied. This is only applied to positional arguments,
            keyword arguments, and return values. Container types will not
            be recursively checked. The callable must be serializable. `None`
            defaults to [`ProxyNever`][proxystore.store.executor.ProxyNever].
    """

    def __init__(
        self,
        executor: Executor,
        store: Store[Any],
        should_proxy: Callable[[Any], bool] | None = None,
    ) -> None:
        if should_proxy is None:
            should_proxy = ProxyNever()

        self.executor = executor
        self.store = store
        self.should_proxy: Callable[[Any], bool] = should_proxy

        self._registered: dict[
            Callable[..., Any],
            _FunctionWrapper[Any, Any],
        ] = {}

    def _wrapped(self, function: Callable[P, R]) -> _FunctionWrapper[P, R]:
        if function not in self._registered:
            self._registered[function] = _FunctionWrapper(
                function,
                store_config=self.store.config(),
                should_proxy=self.should_proxy,
            )

        return cast(_FunctionWrapper[P, R], self._registered[function])

    def submit(
        self,
        function: Callable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[R]:
        """Schedule the callable to be executed.

        Args:
            function: Callable to execute.
            args: Positional arguments.
            kwargs: Keyword arguments.

        Returns:
            [`Future`][concurrent.futures.Future] representing \
            the result of the execution of the callable.
        """
        # We cast the transformed args and kwargs back to P.args and P.kwargs,
        # but note that those types aren't exactly correct. Some items
        # may be Proxy[T] rather than T, but this is not practicle to type.
        args = cast(
            P.args,
            _proxy_iterable(args, self.store, self.should_proxy),
        )
        kwargs = cast(
            P.kwargs,
            _proxy_mapping(kwargs, self.store, self.should_proxy),
        )
        wrapped = self._wrapped(function)
        return self.executor.submit(wrapped, *args, **kwargs)

    def map(
        self,
        function: Callable[P, R],
        *iterables: Iterable[P.args],
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> Iterator[R]:
        """Map a function onto iterables of arguments.

        Args:
            function: A callable that will take as many arguments as there are
                passed iterables.
            iterables: Variable number of iterables.
            timeout: The maximum number of seconds to wait. If None, then there
                is no limit on the wait time.
            chunksize: Sets the Dask batch size.

        Returns:
            An iterator equivalent to: `map(func, *iterables)` but the calls \
            may be evaluated out-of-order.
        """
        iterables = _proxy_iterable(iterables, self.store, self.should_proxy)
        wrapped = self._wrapped(function)
        return self.executor.map(
            wrapped,
            *iterables,
            timeout=timeout,
            chunksize=chunksize,
        )

    def shutdown(
        self,
        wait: bool = True,
        *,
        cancel_futures: bool = False,
    ) -> None:
        """Shutdown the executor and close the store.

        Args:
            wait: Wait on all pending futures to complete.
            cancel_futures: Cancel all pending futures that the executor
                has not started running. Only used in Python 3.9 and later.
        """
        if sys.version_info >= (3, 9):  # pragma: >=3.9 cover
            self.executor.shutdown(wait=wait, cancel_futures=cancel_futures)
        else:  # pragma: <3.9 cover
            self.executor.shutdown(wait=wait)

        self.store.close()
