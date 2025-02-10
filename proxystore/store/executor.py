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
import warnings
from collections.abc import Generator
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from concurrent.futures import Executor
from concurrent.futures import Future
from typing import Any
from typing import Callable
from typing import cast
from typing import Generic
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

from proxystore.proxy import Proxy
from proxystore.store import get_store
from proxystore.store.base import Store
from proxystore.store.config import StoreConfig
from proxystore.store.types import ConnectorKeyT
from proxystore.store.utils import get_key

P = ParamSpec('P')
R = TypeVar('R')


class _FunctionWrapper(Generic[P, R]):
    def __init__(
        self,
        function: Callable[P, R],
        *,
        store_config: StoreConfig,
        should_proxy: Callable[[Any], bool],
        return_owned_proxy: bool,
    ) -> None:
        self.function = function
        self.store_config = store_config
        self.should_proxy = should_proxy
        self.return_owned_proxy = return_owned_proxy

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R | Proxy[R]:
        result = self.function(*args, **kwargs)

        if type(result) is not Proxy and self.should_proxy(result):
            store = self.get_store()
            return (
                store.owned_proxy(result)
                if self.return_owned_proxy
                else store.proxy(result)
            )
        else:
            return result

    def get_store(self) -> Store[Any]:
        store = get_store(self.store_config.name)
        if store is None:
            store = Store.from_config(self.store_config)
        return store


def _proxy_iterable(
    items: Iterable[Any],
    store: Store[Any],
    should_proxy: Callable[[Any], bool],
) -> tuple[tuple[Any, ...], list[ConnectorKeyT]]:
    keys: list[ConnectorKeyT] = []

    def _apply(item: Any) -> Any:
        if type(item) is not Proxy and should_proxy(item):
            proxy = store.proxy(item)
            keys.append(get_key(proxy))
            return proxy
        else:
            return item

    output = tuple(map(_apply, items))
    return output, keys


def _proxy_mapping(
    mapping: Mapping[Any, Any],
    store: Store[Any],
    should_proxy: Callable[[Any], bool],
) -> tuple[dict[Any, Any], list[ConnectorKeyT]]:
    output = {}
    keys: list[ConnectorKeyT] = []

    for key, value in mapping.items():
        if type(value) is not Proxy and should_proxy(value):
            proxy = store.proxy(value)
            keys.append(get_key(proxy))
            output[key] = proxy
        else:
            output[key] = value

    return output, keys


@runtime_checkable
class _FutureProtocol(Protocol[R]):
    """Protocol for future-like objects.

    This [`Protocol`][typing.Protocol] defines an interface that is
    similar to [`concurrent.futures.Future`][concurrent.futures.Future].
    This is helpful for annotating methods that use futures that may not
    inherit from [`concurrent.futures.Future`][concurrent.futures.Future]
    such as Dask's `Future`.

    This protocol does not require `running()` because Dask does not provide
    that method.
    """

    def add_done_callback(
        self,
        callback: Callable[[_FutureProtocol[R]], Any],
    ) -> None:
        """Add a done callback to the future."""
        ...

    def cancel(self) -> bool:
        """Attempt to cancel the task."""
        ...

    def cancelled(self) -> bool:
        """Check if the task was cancelled."""
        ...

    def done(self) -> bool:
        """Check if the task is done."""
        ...

    def exception(self, timeout: float | None = None) -> BaseException | None:
        """Get the exception raised by the task."""
        ...

    def result(self, timeout: float | None = None) -> R:
        """Get the result of the task."""
        ...


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


def _evict_callback(
    store: Store[Any],
    keys: list[ConnectorKeyT],
) -> Callable[[Future[Any]], None]:
    def _callback(_: Future[Any]) -> None:
        for key in keys:
            store.evict(key)

    return _callback


class StoreExecutor(Executor):
    """Executor wrapper that automatically proxies arguments and results.

    By default, the [`StoreExecutor`][proxystore.store.executor.StoreExecutor]
    will automatically manage the memory of proxied objects by evicting
    proxied inputs after execution has completed (via callbacks on the
    futures) and using [Ownership](../../guides/object-lifetimes.md) for
    result values.

    Tip:
        This class is also compatible with some executor-like clients such
        as the Dask Distributed
        [`Client`](https://distributed.dask.org/en/stable/api.html#client).
        While functionally compatible, mypy may consider the usage invalid
        if the specific client does not inherit from
        [`Executor`][concurrent.futures.Executor].

    Warning:
        Proxy [Ownership](../../guides/object-lifetimes.md) may not be
        compatible with every executor type. If you encounter errors such as
        [`ReferenceInvalidError`][proxystore.store.ref.ReferenceInvalidError],
        set `ownership=False` and consider using alternate mechanisms for
        evicted data associated with proxies.

        For example, `ownership=True` is not currently compatible with the
        Dask Distributed `Client` because Dask will maintain multiple
        references to the resulting
        [`OwnedProxy`][proxystore.store.ref.OwnedProxy] which breaks
        the ownership rules.

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
        ownership: Use [`OwnedProxy`][proxystore.store.ref.OwnedProxy] for
            result values rather than [`Proxy`][proxystore.proxy.Proxy] types.
            [`OwnedProxy`][proxystore.store.ref.OwnedProxy] types will
            evict the proxied data from the store when they get garbage
            collected. If `False` and default proxies are used, it is the
            responsibility of the caller to clean up data associated with any
            result proxies.
        close_store: Close `store` when this executor is closed.
    """

    def __init__(
        self,
        executor: Executor,
        store: Store[Any],
        should_proxy: Callable[[Any], bool] | None = None,
        *,
        ownership: bool = True,
        close_store: bool = True,
    ) -> None:
        if should_proxy is None:
            should_proxy = ProxyNever()

        self.executor = executor
        self.store = store
        self.should_proxy: Callable[[Any], bool] = should_proxy
        self.ownership = ownership
        self.close_store = close_store

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
                return_owned_proxy=self.ownership,
            )

        return cast(_FunctionWrapper[P, R], self._registered[function])

    def submit(
        self,
        function: Callable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[R | Proxy[R]]:
        """Schedule the callable to be executed.

        Args:
            function: Callable to execute.
            args: Positional arguments.
            kwargs: Keyword arguments.

        Returns:
            [`Future`][concurrent.futures.Future] representing \
            the result of the execution of the callable.
        """
        pargs, keys1 = _proxy_iterable(args, self.store, self.should_proxy)
        pkwargs, keys2 = _proxy_mapping(kwargs, self.store, self.should_proxy)

        wrapped = self._wrapped(function)
        future = self.executor.submit(wrapped, *pargs, **pkwargs)

        future.add_done_callback(_evict_callback(self.store, keys1 + keys2))
        return future

    def map(  # type: ignore[override]
        self,
        function: Callable[..., R],
        *iterables: Iterable[Any],
        **kwargs: Any,
    ) -> Iterator[R | Proxy[R]]:
        """Map a function onto iterables of arguments.

        Args:
            function: A callable that will take as many arguments as there are
                passed iterables.
            iterables: Variable number of iterables.
            kwargs: Keyword arguments to pass to `self.executor.map()`.

        Returns:
            An iterator equivalent to: `map(func, *iterables)` but the calls \
            may be evaluated out-of-order.
        """
        iterables, keys = _proxy_iterable(
            iterables,
            self.store,
            self.should_proxy,
        )

        wrapped = self._wrapped(function)
        results = self.executor.map(wrapped, *iterables, **kwargs)

        def _result_iterator() -> Generator[R, None, None]:
            for result in results:
                if isinstance(result, _FutureProtocol):
                    # Some Executor-like classes return futures from map()
                    # so we internally handle that here.
                    timeout = kwargs.get('timeout')
                    yield result.result(timeout=timeout)
                else:
                    yield result

            # Wait to evict input proxies until all results have been received.
            # Waiting is needed because there is no guarantee what order tasks
            # complete in.
            for key in keys:
                self.store.evict(key)

        return _result_iterator()

    def shutdown(
        self,
        wait: bool = True,
        *,
        cancel_futures: bool = False,
    ) -> None:
        """Shutdown the executor and close the store.

        Warning:
            This will close the [`Store`][proxystore.store.base.Store] passed
            to this [`StoreExecutor`][proxystore.store.executor.StoreExecutor]
            instance if `close_store=True`, but it is possible the store is
            reinitialized again if `ownership=True` was configured and
            `register=True` was passed to the store. Any
            [`OwnedProxy`][proxystore.store.ref.OwnedProxy]
            instances returned by functions invoked through this executor that
            are still alive will evict themselves once they are garbage
            collected. Eviction requires a store instance so the garbage
            collection processes can inadvertently reinitialize and register
            a store that was previously closed.

        Note:
            Arguments are only used if the wrapped executor is an instance
            of Python's [`Executor`][concurrent.futures.Executor].

        Args:
            wait: Wait on all pending futures to complete.
            cancel_futures: Cancel all pending futures that the executor
                has not started running.
        """
        if isinstance(self.executor, Executor):
            self.executor.shutdown(
                wait=wait,
                cancel_futures=cancel_futures,
            )
        elif hasattr(self.executor, 'close'):
            # Handle Executor-like classes that don't quite follow the
            # Executor protocol, such as the Dask Distributed Client.
            self.executor.close()
        else:
            warnings.warn(
                f'Cannot shutdown {type(self.executor).__name__} because it '
                f'is not a subclass of {Executor.__name__} nor does it have '
                'a close() method.',
                category=RuntimeWarning,
                stacklevel=2,
            )

        if self.close_store:
            self.store.close()
