from __future__ import annotations

import contextlib
import gc
import logging
import multiprocessing
import os
import pathlib
from collections.abc import Callable
from collections.abc import Generator
from collections.abc import Iterable
from collections.abc import Iterator
from concurrent.futures import Executor
from concurrent.futures import Future
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import cast
from typing import ParamSpec
from typing import TypeVar
from unittest import mock

import pytest

import proxystore.store.executor as executor_module
from proxystore.connectors.file import FileConnector
from proxystore.connectors.local import LocalConnector
from proxystore.proxy import Proxy
from proxystore.serialize import SerializationError
from proxystore.serialize import serialize
from proxystore.store._evictions import evictions
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
    base_executor: Executor
    if base_executor_type is ProcessPoolExecutor:
        context = multiprocessing.get_context('spawn')
        # Spawned workers are all started up front, and idle workers that
        # never run a task make coverage warn that no data was collected.
        base_executor = base_executor_type(max_workers=1, mp_context=context)
    else:
        base_executor = base_executor_type()
    store = Store(FileConnector(tmp_path))

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
    base_executor: Executor
    if base_executor_type is ProcessPoolExecutor:
        context = multiprocessing.get_context('spawn')
        # Spawned workers are all started up front, and idle workers that
        # never run a task make coverage warn that no data was collected.
        base_executor = base_executor_type(max_workers=1, mp_context=context)
    else:
        base_executor = base_executor_type()
    store = Store(FileConnector(tmp_path))

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
    evictions.wait()

    # If ownership is enabled, all of the proxied data should have been
    # evicted at this point so the FileConnector directory should
    # be empty.
    if ownership:
        assert len(os.listdir(tmp_path)) == 0

    store.close()


def _is_proxy(x: Any) -> bool:
    return type(x) is Proxy


def test_map_proxies_each_element() -> None:
    store = Store(LocalConnector())

    with StoreExecutor(
        ThreadPoolExecutor(),
        store,
        should_proxy=ProxyType(str),
        ownership=False,
    ) as executor:
        assert executor.submit(_is_proxy, 'a').result()
        assert list(executor.map(_is_proxy, ['a', 1, 'b'])) == [
            True,
            False,
            True,
        ]


def test_map_with_generator() -> None:
    store = Store(LocalConnector())

    with StoreExecutor(
        ThreadPoolExecutor(),
        store,
        should_proxy=ProxyAlways(),
        ownership=False,
    ) as executor:
        results = executor.map(power, (x for x in [1, 2]), [2, 3])
        assert list(results) == [1, 8]


def test_function_wrapper() -> None:
    with Store(LocalConnector()) as store:
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
    store = Store(FileConnector(tmp_path))

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
        raise NotImplementedError

    def map(
        self,
        function: Callable[..., R],
        *iterables: Iterable[Any],
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> Iterator[Future[R]]:
        raise NotImplementedError


def test_warn_unsupported_shutdown_method() -> None:
    store = Store(LocalConnector())
    executor = StoreExecutor(BadExecutor(), store)  # type: ignore[arg-type]
    with pytest.warns(RuntimeWarning, match='Cannot shutdown BadExecutor'):
        executor.shutdown()


@contextlib.contextmanager
def _str_executor(
    base: Executor | None = None,
) -> Generator[tuple[StoreExecutor, LocalConnector], None, None]:
    # Only str inputs are proxied, so the objects in the connector are
    # inputs and not results.
    connector = LocalConnector()
    store = Store(connector)
    base = ThreadPoolExecutor(max_workers=2) if base is None else base
    with StoreExecutor(
        base,
        store,
        should_proxy=ProxyType(str),
        ownership=False,
        close_store=False,
    ) as executor:
        yield executor, connector
    store.close()


def test_function_wrappers_are_bounded() -> None:
    with (
        _str_executor() as (executor, _),
        mock.patch.object(executor_module, '_MAX_WRAPPERS', 2),
    ):
        functions = [lambda x, i=i: x + i for i in range(3)]
        executor.submit(functions[0], 1).result()
        executor.submit(functions[1], 1).result()
        # Using the first function again keeps it over the second.
        executor.submit(functions[0], 1).result()
        executor.submit(functions[2], 1).result()
        assert list(executor._registered) == [functions[0], functions[2]]


def test_submit_error_evicts_inputs() -> None:
    base = ThreadPoolExecutor(max_workers=1)
    base.shutdown()
    with _str_executor(base) as (executor, connector):
        with pytest.raises(RuntimeError, match='shutdown'):
            executor.submit(len, 'value')
        assert len(connector._store) == 0


def test_submit_argument_error_evicts_inputs() -> None:
    with _str_executor() as (executor, connector):
        executor.store._serializer = mock.MagicMock(
            side_effect=[serialize('first'), SerializationError('failed')],
        )

        def _function(first: str, *, kw: str) -> None:
            pass  # pragma: no cover

        with pytest.raises(SerializationError, match='failed'):
            executor.submit(_function, 'first', kw='second')
        assert len(connector._store) == 0


def test_map_error_evicts_inputs() -> None:
    base = ThreadPoolExecutor(max_workers=1)
    base.shutdown()
    with _str_executor(base) as (executor, connector):
        with pytest.raises(RuntimeError, match='shutdown'):
            executor.map(len, ['a', 'b'])
        assert len(connector._store) == 0


def test_map_task_error_evicts_inputs() -> None:
    def _fail(value: str) -> int:
        if value == 'b':
            raise ValueError(value)
        return len(value)

    with _str_executor() as (executor, connector):
        results = executor.map(_fail, ['a', 'b', 'c'])
        assert next(results) == 1
        with pytest.raises(ValueError, match='b'):
            next(results)
        assert len(connector._store) == 0
        with pytest.raises(StopIteration):
            next(results)


def test_map_closed_early_evicts_inputs() -> None:
    with _str_executor() as (executor, connector):
        results = executor.map(len, ['a', 'b', 'c'])
        assert next(results) == 1
        assert isinstance(results, executor_module._MapResults)
        results.close()
        assert len(connector._store) == 0
        # Closing again does nothing.
        results.close()


def test_map_deleted_early_evicts_inputs() -> None:
    with _str_executor() as (executor, connector):
        results = executor.map(len, ['a', 'b', 'c'])
        assert len(connector._store) == 3
        del results
        gc.collect()
        evictions.wait()
        assert len(connector._store) == 0


def test_evict_errors_are_logged(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.ERROR)
    with _str_executor() as (executor, connector):
        with mock.patch.object(
            executor.store,
            'evict',
            side_effect=RuntimeError('failed'),
        ):
            assert list(executor.map(len, ['a', 'b'])) == [1, 1]
        assert caplog.text.count('Failed to evict input') == 2
        connector._store.clear()


def test_map_closed_early_with_dask_like_client() -> None:
    client: Any = DaskLikeClient()
    with _str_executor(client) as (executor, connector):
        results = executor.map(len, ['a', 'b'])
        assert next(results) == 1
        # The results of the client are a list iterator without close().
        cast(Any, results).close()
        assert len(connector._store) == 0
