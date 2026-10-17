from __future__ import annotations

import logging
import pathlib
import threading
from unittest import mock

import pytest

from proxystore.connectors.file import FileConnector
from proxystore.connectors.local import LocalConnector
from proxystore.connectors.local import LocalKey
from proxystore.store._evictions import _Evictions
from proxystore.store._evictions import evictions
from proxystore.store.base import Store
from proxystore.store.factory import StoreFactory
from proxystore.store.registry import registry


def test_delete_owned_proxy_while_holding_registry_lock() -> None:
    with Store(LocalConnector()) as store:
        proxy = store.owned_proxy('value')
        key = proxy.__proxy_factory__.key

        # Before evictions were queued, deleting an owned proxy while this
        # thread held a lock needed to evict (e.g., when the garbage
        # collector deletes it) waited on itself forever.
        with registry._lock:
            del proxy
        evictions.wait()

        assert not store.exists(key)


def test_close_waits_for_evictions() -> None:
    connector = LocalConnector()
    store = Store(connector)
    proxy = store.owned_proxy('value')
    key = proxy.__proxy_factory__.key

    calls: list[str] = []
    started = threading.Event()
    release = threading.Event()
    evict = store.evict
    close = connector.close

    def _slow_evict(key: LocalKey) -> None:
        started.set()
        release.wait()
        evict(key)
        calls.append('evict')

    def _close(**kwargs: object) -> bool:
        calls.append('close')
        return close(**kwargs)  # type: ignore[arg-type]

    with (
        mock.patch.object(store, 'evict', side_effect=_slow_evict),
        mock.patch.object(connector, 'close', side_effect=_close),
    ):
        del proxy
        assert started.wait(timeout=5)
        closer = threading.Thread(target=store.close, kwargs={'clear': False})
        closer.start()
        release.set()
        closer.join(timeout=5)

    assert not closer.is_alive()
    assert calls == ['evict', 'close']
    assert not connector.exists(key)


def test_wait_from_eviction_thread_returns() -> None:
    with Store(LocalConnector()) as store:
        proxy = store.owned_proxy('value')
        key = proxy.__proxy_factory__.key
        evict = store.evict

        def _evict(key: LocalKey) -> None:
            # Waiting in the eviction thread would wait on itself.
            evictions.wait()
            evict(key)

        with mock.patch.object(store, 'evict', side_effect=_evict):
            del proxy
            evictions.wait()

        assert not store.exists(key)


def test_wait_without_thread_evicts_in_caller() -> None:
    queue = _Evictions()
    with Store(LocalConnector()) as store:
        key = store.put('value')
        queue.put(StoreFactory(key, store.config()))
        assert store.exists(key)

        queue.wait()
        assert not store.exists(key)


def test_eviction_errors_are_logged(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.ERROR)
    queue = _Evictions()
    with Store(LocalConnector()) as store:
        factory: StoreFactory[LocalConnector, str] = StoreFactory(
            store.put('value'),
            store.config(),
        )

        with mock.patch.object(store, 'evict', side_effect=RuntimeError):
            queue.put(factory)
            queue.wait()
        assert 'Failed to evict' in caplog.text

        with mock.patch.object(
            factory,
            'get_store',
            side_effect=RuntimeError,
        ):
            queue.put(factory)
            queue.wait()
        assert 'Failed to get the store' in caplog.text


def test_evict_after_owner_cleared(tmp_path: pathlib.Path) -> None:
    store = Store(FileConnector(str(tmp_path / 'store'), clear=True))
    proxy = store.owned_proxy('value')
    store.close(clear=True)

    # The store ID is closed so there is nothing to evict.
    del proxy
    evictions.wait()


def test_reset_after_fork() -> None:
    queue = _Evictions()
    queue._reset_after_fork()
    assert queue._thread is None

    queue.start()
    thread = queue._thread
    queue._reset_after_fork()
    assert queue._thread is not None
    assert queue._thread is not thread
