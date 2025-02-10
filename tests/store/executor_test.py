from __future__ import annotations

import gc
import multiprocessing
import os
import pathlib
import sys
from collections.abc import Iterable
from collections.abc import Iterator
from concurrent.futures import Executor
from concurrent.futures import Future
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Callable
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

import pytest

from proxystore.connectors.file import FileConnector
from proxystore.connectors.local import LocalConnector
from proxystore.proxy import Proxy
from proxystore.store import unregister_store
from proxystore.store.base import Store
from proxystore.store.executor import _FunctionWrapper
from proxystore.store.executor import ProxyAlways
from proxystore.store.executor import ProxyNever
from proxystore.store.executor import ProxyType
from proxystore.store.executor import StoreExecutor

P = ParamSpec('P')
R = TypeVar('R')


def power(x: int, exp: int = 2) -> int:
    return x**exp


@pytest.mark.parametrize(
    'base_executor_type',
    (ThreadPoolExecutor, ProcessPoolExecutor),
)
def test_default_behavior(
    base_executor_type: type[Executor],
    tmp_path: pathlib.Path,
) -> None:
    if base_executor_type is ProcessPoolExecutor:
        context = multiprocessing.get_context('spawn')
        base_executor = base_executor_type(mp_context=context)  # type: ignore[call-arg]
    else:
        base_executor = base_executor_type()
    store = Store(
        'test-default-behavior',
        FileConnector(str(tmp_path)),
        register=True,
    )

    with StoreExecutor(base_executor, store) as executor:
        assert isinstance(executor.should_proxy, ProxyNever)

        future = executor.submit(sum, [1, 2, 3], start=-6)
        result = future.result()
        assert not isinstance(result, Proxy)
        assert result == 0

        # Submit same function again to test function wrapping happens once.
        assert executor.submit(sum, [1, -1]).result() == 0

        results = list(executor.map(power, [1, -1, 2]))
        assert not any(isinstance(value, Proxy) for value in results)
        assert results == [1, 1, 4]


@pytest.mark.parametrize(
    ('base_executor_type', 'ownership'),
    (
        (ThreadPoolExecutor, True),
        (ProcessPoolExecutor, True),
        (ProcessPoolExecutor, False),
    ),
)
def test_proxy_behavior(
    base_executor_type: type[Executor],
    ownership: bool,
    tmp_path: pathlib.Path,
) -> None:
    if base_executor_type is ProcessPoolExecutor:
        context = multiprocessing.get_context('spawn')
        base_executor = base_executor_type(mp_context=context)  # type: ignore[call-arg]
    else:
        base_executor = base_executor_type()
    store = Store(
        'test-proxy-behavior',
        FileConnector(str(tmp_path)),
        register=True,
    )

    with StoreExecutor(
        base_executor,
        store,
        should_proxy=ProxyAlways(),
        ownership=ownership,
        close_store=False,
    ) as executor:
        future = executor.submit(sum, [1, 2, 3], start=-6)
        result = future.result()

        assert isinstance(result, Proxy)
        assert result == 0

        # Submit same function again to test function wrapping happens once.
        assert executor.submit(sum, [1, -1]).result() == 0

        results = list(executor.map(power, [1, -1, 2]))
        assert all(isinstance(value, Proxy) for value in results)
        assert results == [1, 1, 4]

    # Delete any proxies before we close the store. This is to prevent
    # the __del__ method of any OwnedProxies from reinitializing the store.
    del future
    del result
    del results

    # Run the garbage collector to guarantee that __del__ is called on the
    # returned OwnedProxies.
    gc.collect()

    # If ownership is enabled, all of the proxied data should have been
    # evicted at this point so the FileConnector directory should
    # be empty.
    if ownership:
        assert len(os.listdir(tmp_path)) == 0

    store.close()


def test_function_wrapper() -> None:
    with Store(
        'test-function-wrapper',
        LocalConnector(),
        register=False,
    ) as store:
        wrapped = _FunctionWrapper(
            power,
            store_config=store.config(),
            should_proxy=ProxyNever(),
            return_owned_proxy=False,
        )
        assert wrapped(2, exp=3) == 8

        wrapped = _FunctionWrapper(
            power,
            store_config=store.config(),
            should_proxy=ProxyAlways(),
            return_owned_proxy=False,
        )
        assert wrapped(2, exp=3) == 8

        unregister_store(store)


@pytest.mark.parametrize(
    ('should_proxy', 'obj', 'should'),
    (
        (ProxyAlways(), 42, True),
        (ProxyNever(), 42, False),
        (ProxyType(int, str), 42, True),
        (ProxyType(int, str), '42', True),
        (ProxyType(int, str), [42], False),
    ),
)
def test_should_proxy(
    should_proxy: Callable[[Any], bool],
    obj: Any,
    should: bool,
) -> None:
    assert should_proxy(obj) == should


class DaskLikeClient:
    def submit(
        self,
        function: Callable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[R]:
        future: Future[R] = Future()
        future.set_result(function(*args, **kwargs))
        return future

    def map(
        self,
        function: Callable[..., R],
        *iterables: Iterable[Any],
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> Iterator[Future[R]]:
        futures: list[Future[R]] = []
        for result in map(function, *iterables):
            future: Future[R] = Future()
            future.set_result(result)
            futures.append(future)

        return iter(futures)

    def close(self) -> None:
        pass


def test_dask_like_client(tmp_path: pathlib.Path) -> None:
    store = Store(
        'test-dask-like-client',
        FileConnector(str(tmp_path)),
        register=True,
    )

    with StoreExecutor(
        DaskLikeClient(),  # type: ignore[arg-type]
        store,
    ) as executor:
        future = executor.submit(sum, [1, 2, 3], start=-6)
        assert future.result() == 0

        results = executor.map(power, [1, -1, 2])
        assert list(results) == [1, 1, 4]


class BadExecutor:
    def submit(
        self,
        function: Callable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[R]:
        raise NotImplementedError()

    def map(
        self,
        function: Callable[..., R],
        *iterables: Iterable[Any],
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> Iterator[Future[R]]:
        raise NotImplementedError()


def test_warn_unsupported_shutdown_method() -> None:
    store = Store('test-function-wrapper', LocalConnector(), register=False)
    executor = StoreExecutor(BadExecutor(), store)  # type: ignore[arg-type]
    with pytest.warns(RuntimeWarning, match='Cannot shutdown BadExecutor'):
        executor.shutdown()
