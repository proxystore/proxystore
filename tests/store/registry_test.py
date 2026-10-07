from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from proxystore.connectors.local import LocalConnector
from proxystore.store.base import Store
from proxystore.store.config import StoreConfig
from proxystore.store.registry import registry
from proxystore.store.registry import StoreRegistry


def test_registry_lookup() -> None:
    assert len(registry) == 0
    with Store(LocalConnector()) as store:
        assert len(registry) == 1
        assert store.id in registry
        assert registry.get(store.id) is store
    assert len(registry) == 0
    assert store.id not in registry
    assert registry.get(store.id) is None


def test_registry_iter_and_clear() -> None:
    store_registry = StoreRegistry()
    stores = [Store(LocalConnector()) for _ in range(2)]
    for store in stores:
        store_registry.register(store)
    assert set(store_registry) == {store.id for store in stores}

    store_registry.clear()
    assert len(store_registry) == 0
    assert list(store_registry) == []

    for store in stores:
        store.close()


def test_get_or_create_without_id() -> None:
    with Store(LocalConnector()) as store:
        config = store.config().model_copy(update={'id': None})
        new_store = registry.get_or_create(config)
        assert new_store.id != store.id
        assert not new_store.owner
        assert registry.get(new_store.id) is new_store
        new_store.close()


def _closed_config() -> StoreConfig:
    # Config of a store that is no longer registered, so get_or_create()
    # has to make a new store.
    store = Store(LocalConnector())
    config = store.config()
    store.close()
    return config


class _SlowFromConfig:
    """Store.from_config() that waits for an event for some store IDs."""

    def __init__(self, slow_ids: set[str]) -> None:
        self.slow_ids = slow_ids
        self.started = threading.Event()
        self.release = threading.Event()
        self.calls: list[str | None] = []
        self._from_config = Store.from_config

    def __call__(self, config: StoreConfig, **kwargs: Any) -> Store[Any]:
        self.calls.append(config.id)
        if config.id in self.slow_ids:
            self.started.set()
            assert self.release.wait(5)
        return self._from_config(config, **kwargs)


def test_making_one_store_does_not_block_others(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    slow_config = _closed_config()
    fast_config = _closed_config()
    assert slow_config.id is not None
    slow = _SlowFromConfig({slow_config.id})
    monkeypatch.setattr(Store, 'from_config', slow)

    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(registry.get_or_create, slow_config)
        assert slow.started.wait(5)

        # The slow store is still being made, but this does not wait for it.
        fast_store = registry.get_or_create(fast_config)
        assert not future.done()

        slow.release.set()
        slow_store = future.result(timeout=5)

    assert fast_store.id == fast_config.id
    assert slow_store.id == slow_config.id
    fast_store.close()
    slow_store.close()


def test_same_store_made_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _closed_config()
    assert config.id is not None
    slow = _SlowFromConfig({config.id})
    monkeypatch.setattr(Store, 'from_config', slow)

    with ThreadPoolExecutor(2) as pool:
        future1 = pool.submit(registry.get_or_create, config)
        assert slow.started.wait(5)
        future2 = pool.submit(registry.get_or_create, config)
        slow.release.set()
        store1 = future1.result(timeout=5)
        store2 = future2.result(timeout=5)

    assert store1 is store2
    assert slow.calls == [config.id]
    store1.close()


def test_store_registered_while_making(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _closed_config()
    from_config = Store.from_config
    registered: list[Store[Any]] = []

    def _from_config(config: StoreConfig, **kwargs: Any) -> Store[Any]:
        # Another thread makes a store with the same ID directly while
        # this store is being made.
        registered.append(Store(LocalConnector(), _id=config.id))
        return from_config(config, **kwargs)

    monkeypatch.setattr(Store, 'from_config', _from_config)

    store = registry.get_or_create(config)
    assert store is registered[0]
    assert config.id is not None
    assert registry.get(config.id) is store
    store.close()
