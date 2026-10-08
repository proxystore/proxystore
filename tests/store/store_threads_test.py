from __future__ import annotations

import contextlib
import threading
from collections.abc import Generator
from collections.abc import Sequence
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from unittest import mock

import pytest

from proxystore.connectors.local import LocalConnector
from proxystore.connectors.local import LocalKey
from proxystore.serialize import BytesLike
from proxystore.serialize import deserialize
from proxystore.serialize import SerializationError
from proxystore.store import Store
from proxystore.store.base import _reset_stores_after_fork

TIMEOUT = 5


class Blocker:
    """Makes the gets of a connector wait until released."""

    def __init__(self, connector: LocalConnector) -> None:
        # Released once by each get that starts.
        self.entered = threading.Semaphore(0)
        self.release = threading.Event()
        self.error: Exception | None = None
        self.calls: list[LocalKey] = []
        self._get = connector.get

    def _wait(self, keys: Sequence[LocalKey]) -> None:
        self.calls.extend(keys)
        self.entered.release()
        assert self.release.wait(TIMEOUT)
        if self.error is not None:
            raise self.error

    def get(self, key: LocalKey) -> BytesLike | None:
        self._wait([key])
        return self._get(key)

    def get_batch(self, keys: Sequence[LocalKey]) -> list[BytesLike | None]:
        self._wait(keys)
        return [self._get(key) for key in keys]


class _Future(Future[Any]):
    # Sets waiting when a thread waits on a fetch of another thread.
    waiting = threading.Event()

    def result(self, timeout: float | None = None) -> Any:
        _Future.waiting.set()
        return super().result(timeout)


@contextlib.contextmanager
def blocking_store(
    cache_size: int = 0,
) -> Generator[tuple[Store[LocalConnector], Blocker], None, None]:
    _Future.waiting.clear()
    connector = LocalConnector()
    blocker = Blocker(connector)
    with (
        mock.patch.object(connector, 'get', blocker.get),
        mock.patch.object(connector, 'get_batch', blocker.get_batch),
        mock.patch('proxystore.store.cache.Future', _Future),
        Store(connector, cache_size=cache_size) as store,
    ):
        yield store, blocker


def _start(fn: Any, *args: Any, **kwargs: Any) -> Future[Any]:
    pool = ThreadPoolExecutor(1)
    future = pool.submit(fn, *args, **kwargs)
    pool.shutdown(wait=False)
    return future


def test_get_different_keys_at_same_time() -> None:
    barrier = threading.Barrier(2, timeout=TIMEOUT)
    connector = LocalConnector()
    get = connector.get

    def _get(key: LocalKey) -> BytesLike | None:
        # Raises BrokenBarrierError if the gets run one at a time.
        barrier.wait()
        return get(key)

    with (
        mock.patch.object(connector, 'get', _get),
        Store(connector, cache_size=0) as store,
    ):
        keys = [store.put(i) for i in range(2)]
        with ThreadPoolExecutor(2) as pool:
            assert list(pool.map(store.get, keys)) == [0, 1]


def test_get_same_key_fetched_once() -> None:
    with blocking_store() as (store, blocker):
        key = store.put([1, 2, 3])

        first = _start(store.get, key)
        assert blocker.entered.acquire(timeout=TIMEOUT)
        second = _start(store.get, key)
        assert _Future.waiting.wait(TIMEOUT)
        blocker.release.set()

        assert first.result(TIMEOUT) == [1, 2, 3]
        assert second.result(TIMEOUT) is first.result()
        assert blocker.calls == [key]


def test_get_same_key_other_deserializer_fetched_again() -> None:
    def _deserializer(b: BytesLike) -> Any:
        return deserialize(b)

    with blocking_store() as (store, blocker):
        key = store.put('value')

        first = _start(store.get, key)
        assert blocker.entered.acquire(timeout=TIMEOUT)
        # The second get does its own fetch, which is also blocked.
        second = _start(store.get, key, deserializer=_deserializer)
        blocker.release.set()

        assert first.result(TIMEOUT) == second.result(TIMEOUT) == 'value'
        assert blocker.calls == [key, key]
        assert not _Future.waiting.is_set()


def test_get_waiter_gets_fetch_error() -> None:
    with blocking_store() as (store, blocker):
        key = store.put('value')
        blocker.error = RuntimeError('fetch failed')

        first = _start(store.get, key)
        assert blocker.entered.acquire(timeout=TIMEOUT)
        second = _start(store.get, key)
        assert _Future.waiting.wait(TIMEOUT)
        blocker.release.set()

        with pytest.raises(RuntimeError, match='fetch failed'):
            first.result(TIMEOUT)
        with pytest.raises(RuntimeError, match='fetch failed'):
            second.result(TIMEOUT)


@pytest.mark.parametrize('method', ('evict', 'set'))
def test_change_during_fetch_not_cached(method: str) -> None:
    with blocking_store(cache_size=16) as (store, blocker):
        key = store.put('old')

        get = _start(store.get, key)
        assert blocker.entered.acquire(timeout=TIMEOUT)
        if method == 'evict':
            store.evict(key)
        else:
            store._set(key, 'new')
        blocker.release.set()

        # The fetch started before the change so its result, which may be
        # the old object, must not be cached.
        get.result(TIMEOUT)
        assert not store.is_cached(key)


def test_get_batch_waits_on_get() -> None:
    with blocking_store() as (store, blocker):
        key1 = store.put('value1')
        key2 = store.put('value2')
        missing = LocalKey('missing')

        get1 = _start(store.get, key1)
        get2 = _start(store.get, missing)
        assert blocker.entered.acquire(timeout=TIMEOUT)
        assert blocker.entered.acquire(timeout=TIMEOUT)
        batch = _start(store.get_batch, [key1, missing, key2], default='d')
        # The batch fetches only key2 and waits on the other fetches.
        assert blocker.entered.acquire(timeout=TIMEOUT)
        blocker.release.set()

        assert get1.result(TIMEOUT) == 'value1'
        assert get2.result(TIMEOUT) is None
        assert batch.result(TIMEOUT) == ['value1', 'd', 'value2']
        assert sorted(blocker.calls[:2]) == sorted([key1, missing])
        assert blocker.calls[2:] == [key2]
        assert _Future.waiting.is_set()


def test_get_waits_on_get_batch() -> None:
    with blocking_store() as (store, blocker):
        key = store.put('value')
        missing = LocalKey('missing')

        batch = _start(store.get_batch, [key, missing], default='default')
        assert blocker.entered.acquire(timeout=TIMEOUT)
        get = _start(store.get, missing, default='other')
        assert _Future.waiting.wait(TIMEOUT)
        blocker.release.set()

        assert batch.result(TIMEOUT) == ['value', 'default']
        assert get.result(TIMEOUT) == 'other'
        assert blocker.calls == [key, missing]


def test_get_batch_error_fails_waiters() -> None:
    with blocking_store() as (store, blocker):
        key = store.put('value')
        blocker.error = RuntimeError('fetch failed')

        batch = _start(store.get_batch, [key])
        assert blocker.entered.acquire(timeout=TIMEOUT)
        get = _start(store.get, key)
        assert _Future.waiting.wait(TIMEOUT)
        blocker.release.set()

        with pytest.raises(RuntimeError, match='fetch failed'):
            batch.result(TIMEOUT)
        with pytest.raises(RuntimeError, match='fetch failed'):
            get.result(TIMEOUT)


def test_get_batch_deserializer_error_finishes_other_keys() -> None:
    def _deserializer(b: BytesLike) -> Any:
        if b == b'bad':
            raise ValueError('bad')
        return deserialize(b)

    with Store(LocalConnector()) as store:
        key1 = store.put('value1', serializer=lambda s: b'bad')
        key2 = store.put('value2')

        with pytest.raises(SerializationError):
            store.get_batch([key1, key2], deserializer=_deserializer)

        assert store.is_cached(key2)
        assert not store.is_cached(key1)


def test_store_after_fork() -> None:
    with blocking_store(cache_size=16) as (store, blocker):
        key = store.put('value')

        # Another thread is fetching the key and using the cache when the
        # process is forked.
        get = _start(store.get, key)
        assert blocker.entered.acquire(timeout=TIMEOUT)
        assert store.cache._lock.acquire(timeout=TIMEOUT)

        # Simulate the fork handler running in a forked child process
        _reset_stores_after_fork()
        assert not store.cache._lock.locked()

        # Gets in the child do not wait on the parent's fetch or locks.
        blocker.release.set()
        assert store.get(key) == 'value'

        get.result(TIMEOUT)
