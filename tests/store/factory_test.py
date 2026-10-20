from __future__ import annotations

import threading
from concurrent.futures import Future
from typing import Any
from unittest import mock

import pytest

import proxystore.store.factory as factory_module
from proxystore.connectors.local import LocalConnector
from proxystore.serialize import deserialize
from proxystore.serialize import serialize
from proxystore.store import Store
from proxystore.store.exceptions import ProxyResolveMissingKeyError
from proxystore.store.factory import PollingStoreFactory
from proxystore.store.factory import StoreFactory
from proxystore.store.future import PollingPolicy
from proxystore.store.types import CacheModeT

FactoryT = PollingStoreFactory[LocalConnector, str]


def test_polling_store_factory_resolve() -> None:
    with Store(LocalConnector()) as store:
        value = 'test-value'
        key = store.put(value)
        factory: FactoryT = PollingStoreFactory(
            key=key,
            store_config=store.config(),
        )
        assert factory.resolve() == value
        assert factory() == value


def test_polling_store_factory_evict() -> None:
    with Store(LocalConnector()) as store:
        value = 'test-value'
        key = store.put(value)
        factory: FactoryT = PollingStoreFactory(
            key=key,
            store_config=store.config(),
            evict=True,
        )
        assert factory.resolve() == value
        assert not store.exists(key)


def test_polling_store_factory_metrics() -> None:
    with Store(LocalConnector(), metrics=True) as store:
        value = 'test-value'
        key = store.put(value)
        factory: FactoryT = PollingStoreFactory(
            key=key,
            store_config=store.config(),
        )
        assert factory.resolve() == value

        assert store.metrics is not None
        metrics = store.metrics.get_metrics(key)
        assert metrics is not None
        assert 'factory.polling_resolve' in metrics.times


def test_polling_store_factory_timeout() -> None:
    with Store(LocalConnector()) as store:
        key = store.connector.new_key()
        factory: FactoryT = PollingStoreFactory(
            key=key,
            store_config=store.config(),
            evict=True,
            polling=PollingPolicy(interval=0.001, timeout=0.002),
        )
        with pytest.raises(
            ProxyResolveMissingKeyError,
        ):  # pragma: <3.14 cover
            factory.resolve()


def test_polling_store_factory_backoff() -> None:
    with Store(LocalConnector()) as store:
        key = store.connector.new_key()
        factory: FactoryT = PollingStoreFactory(
            key=key,
            store_config=store.config(),
            evict=True,
            polling=PollingPolicy(
                interval=0.25,
                backoff_factor=2,
                interval_limit=1,
                timeout=2.5,
            ),
        )
        with (
            mock.patch('proxystore.store.factory.time.sleep') as mock_sleep,
            pytest.raises(ProxyResolveMissingKeyError),
        ):
            factory.resolve()

        # The interval doubles up to the limit until the total time slept
        # reaches the timeout.
        assert mock_sleep.call_args_list == [
            mock.call(0.25),
            mock.call(0.5),
            mock.call(1),
            mock.call(1),
        ]


def test_polling_store_factory_serialize() -> None:
    with Store(LocalConnector()) as store:
        value = 'test-value'
        key = store.put(value)
        factory: FactoryT = PollingStoreFactory(
            key=key,
            store_config=store.config(),
        )
        factory = deserialize(serialize(factory))
        assert factory.resolve() == value
        assert factory() == value


class _WaitedEvent(threading.Event):
    # Set when a thread starts waiting on any event of this type.
    waiting = threading.Event()

    def wait(self, timeout: float | None = None) -> bool:
        self.waiting.set()
        return super().wait(timeout)


def _start_second_call(
    factory: StoreFactory[Any, Any],
    started: threading.Event,
) -> tuple[threading.Thread, list[Any]]:
    # Waits until the first call is resolving, then calls the factory in
    # another thread and waits until that thread waits on the first call.
    results: list[Any] = []

    def _call() -> None:
        try:
            results.append(factory())
        except RuntimeError as e:
            results.append(e)

    assert started.wait(timeout=5)
    _WaitedEvent.waiting.clear()
    thread = threading.Thread(target=_call)
    thread.start()
    assert _WaitedEvent.waiting.wait(timeout=5)
    return thread, results


@pytest.mark.parametrize('cache_mode', ('objects', 'bytes'))
def test_concurrent_calls_resolve_once(cache_mode: CacheModeT) -> None:
    with Store(LocalConnector(), cache_mode=cache_mode) as store:
        factory: StoreFactory[Any, list[int]] = StoreFactory(
            store.put([1, 2, 3]),
            store.config(),
        )
        started = threading.Event()
        release = threading.Event()
        resolve = factory.resolve
        calls = 0

        def _resolve() -> list[int]:
            nonlocal calls
            calls += 1
            started.set()
            release.wait()
            return resolve()

        first: list[Any] = []
        with (
            mock.patch.object(factory, 'resolve', side_effect=_resolve),
            mock.patch.object(factory_module, 'Event', _WaitedEvent),
        ):
            thread = threading.Thread(target=lambda: first.append(factory()))
            thread.start()
            try:
                second, results = _start_second_call(factory, started)
            finally:
                release.set()
            thread.join(timeout=5)
            second.join(timeout=5)

        assert calls == 1
        assert first == [[1, 2, 3]]
        # Both calls get the same object, even when the store deserializes
        # the object for every get().
        assert results[0] is first[0]
        assert factory._resolve is None


def test_concurrent_calls_get_the_error() -> None:
    with Store(LocalConnector()) as store:
        factory: StoreFactory[Any, list[int]] = StoreFactory(
            store.put([1, 2, 3]),
            store.config(),
        )
        started = threading.Event()
        release = threading.Event()

        def _resolve() -> list[int]:
            started.set()
            release.wait()
            raise RuntimeError('failed')

        first: list[Any] = []

        def _call() -> None:
            try:
                factory()
            except RuntimeError as e:
                first.append(e)

        with (
            mock.patch.object(factory, 'resolve', side_effect=_resolve),
            mock.patch.object(factory_module, 'Event', _WaitedEvent),
        ):
            thread = threading.Thread(target=_call)
            thread.start()
            try:
                second, results = _start_second_call(factory, started)
            finally:
                release.set()
            thread.join(timeout=5)
            second.join(timeout=5)

        assert len(first) == 1
        assert results == [first[0]]
        # A later call resolves again.
        assert factory() == [1, 2, 3]


def test_call_while_resolving_in_same_thread() -> None:
    with Store(LocalConnector()) as store:
        factory: StoreFactory[Any, list[int]] = StoreFactory(
            store.put([1, 2, 3]),
            store.config(),
        )
        resolve = factory.resolve
        inner: list[list[int]] = []
        depth = 0

        def _resolve() -> list[int]:
            nonlocal depth
            depth += 1
            # E.g., a deserializer which uses the proxy being resolved.
            if depth == 1:
                inner.append(factory())
            return resolve()

        with mock.patch.object(factory, 'resolve', side_effect=_resolve):
            assert factory() == [1, 2, 3]
        assert inner == [[1, 2, 3]]


def test_resolve_async_once_while_in_progress() -> None:
    with Store(LocalConnector()) as store:
        factory: StoreFactory[Any, list[int]] = StoreFactory(
            store.put([1, 2, 3]),
            store.config(),
        )
        with mock.patch.object(
            factory_module,
            '_get_default_pool',
        ) as get_pool:
            get_pool.return_value.submit.return_value = Future()
            factory.resolve_async()
            factory.resolve_async()
        get_pool.return_value.submit.assert_called_once()


def test_concurrent_calls_wait_on_resolve_async() -> None:
    class _CountedFuture(Future[Any]):
        waiters = 0
        two_waiting = threading.Event()

        def result(self, timeout: float | None = None) -> Any:
            with _lock:
                type(self).waiters += 1
                if type(self).waiters == 2:
                    self.two_waiting.set()
            return super().result(timeout)

    _lock = threading.Lock()
    with Store(LocalConnector()) as store:
        factory: StoreFactory[Any, list[int]] = StoreFactory(
            store.put([1, 2, 3]),
            store.config(),
        )
        future = _CountedFuture()
        with mock.patch.object(factory_module, '_get_default_pool') as pool:
            pool.return_value.submit.return_value = future
            factory.resolve_async()

        results: list[list[int]] = []
        threads = [
            threading.Thread(target=lambda: results.append(factory()))
            for _ in range(2)
        ]
        for thread in threads:
            thread.start()
        try:
            assert _CountedFuture.two_waiting.wait(timeout=5)
        finally:
            future.set_result([1, 2, 3])
        for thread in threads:
            thread.join(timeout=5)

        assert results == [[1, 2, 3], [1, 2, 3]]
        assert results[0] is results[1]
        assert factory._resolve is None


def test_resolve_started_before_fork_is_replaced() -> None:
    with Store(LocalConnector()) as store:
        factory: StoreFactory[Any, list[int]] = StoreFactory(
            store.put([1, 2, 3]),
            store.config(),
        )
        started = threading.Event()
        release = threading.Event()
        resolve = factory.resolve

        def _resolve() -> list[int]:
            if not started.is_set():
                started.set()
                release.wait()
            return resolve()

        first: list[list[int]] = []
        with mock.patch.object(factory, 'resolve', side_effect=_resolve):
            thread = threading.Thread(target=lambda: first.append(factory()))
            thread.start()
            try:
                assert started.wait(timeout=5)
                # Simulate the fork handler running in a forked child, where
                # the resolve in progress never finishes.
                factory_module._reset_resolve_lock_after_fork()
                assert factory() == [1, 2, 3]
            finally:
                release.set()
            thread.join(timeout=5)

        assert first == [[1, 2, 3]]
        assert factory._resolve is None
