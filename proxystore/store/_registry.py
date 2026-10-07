"""Registry of the stores in this process.

The registry is keyed by the unique [`id`][proxystore.store.base.Store.id]
of each store. A store registers itself when it is created and unregisters
itself when it is closed.

This module only imports [`Store`][proxystore.store.base.Store] when a
store needs to be made because `proxystore.store.base` imports this module.
"""

from __future__ import annotations

import logging
import threading
from typing import Any
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from proxystore.store.base import Store
    from proxystore.store.config import StoreConfig

logger = logging.getLogger(__name__)

_stores: dict[str, Store[Any]] = {}
# Only held to read or change _stores and _create_locks, never while a
# store is being made, so making one store does not block the others.
_lock = threading.Lock()
# One lock per store ID, held while get_or_create() makes that store, so
# threads that need the same store make it only once. Locks are never
# removed because a process only ever sees a few store IDs.
_create_locks: dict[str, threading.Lock] = {}


def get_or_create(config: StoreConfig) -> Store[Any]:
    """Get the registered store with the ID in `config` or make a new one.

    The new store is not the [`owner`][proxystore.store.base.Store.owner]
    of the objects stored by its connector.
    """
    from proxystore.store.base import Store

    if config.id is None:
        return Store.from_config(config, owner=False)

    with _lock:
        store = _stores.get(config.id)
        if store is not None:
            return store
        create_lock = _create_locks.setdefault(config.id, threading.Lock())

    with create_lock:
        # Another thread may have made the store while this one waited.
        with _lock:
            store = _stores.get(config.id)
        if store is not None:
            return store

        new_store = Store.from_config(config, owner=False)
        with _lock:
            registered = _stores.get(config.id)
        if registered is None or registered is new_store:
            return new_store

        # A store with this ID was made directly with the Store constructor
        # while the connector was being built. Use the registered store so
        # every caller gets the same instance.
        new_store.close()
        return registered


def register(store: Store[Any]) -> None:
    """Register a store if no store with the same ID is registered."""
    with _lock:
        if store.id not in _stores:
            _stores[store.id] = store
            logger.debug('Registered %r', store)


def unregister(store: Store[Any]) -> None:
    """Unregister a store if this instance is the registered store."""
    with _lock:
        if _stores.get(store.id) is store:
            del _stores[store.id]
            logger.debug('Unregistered %r', store)
