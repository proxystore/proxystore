"""Registry of the stores in a process.

Every [`Store`][proxystore.store.base.Store] registers itself in the
module-level [`registry`][proxystore.store.registry.registry] when it is
created and unregisters itself when it is
[closed][proxystore.store.base.Store.close]. Stores are keyed by their
unique [`id`][proxystore.store.base.Store.id], so a proxy resolved in
the same process as the store which made it reuses that store, and
proxies of a store from another process share one new store.

Most code does not need this module. Use
[`get_store()`][proxystore.store.get_store] or
[`get_or_create_store()`][proxystore.store.get_or_create_store] instead.
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


class StoreRegistry:
    """Registry of stores keyed by store ID.

    This class is thread-safe. Making a new store in
    [`get_or_create()`][proxystore.store.registry.StoreRegistry.get_or_create]
    does not block threads that look up or make other stores.
    """

    def __init__(self) -> None:
        self._stores: dict[str, Store[Any]] = {}
        # Only held to read or change _stores and _create_locks, never
        # while a store is being made.
        self._lock = threading.Lock()
        # One lock per store ID, held while get_or_create() makes that
        # store, so threads that need the same store make it only once.
        # Locks are never removed because a process only sees a few IDs.
        self._create_locks: dict[str, threading.Lock] = {}

    def __contains__(self, store_id: object) -> bool:
        with self._lock:
            return store_id in self._stores

    def __len__(self) -> int:
        with self._lock:
            return len(self._stores)

    def get(self, store_id: str) -> Store[Any] | None:
        """Get the registered store with an ID.

        Returns:
            The store or `None` if no store with the ID is registered.
        """
        with self._lock:
            return self._stores.get(store_id)

    def get_or_create(self, config: StoreConfig) -> Store[Any]:
        """Get the registered store with the ID in `config` or make one.

        The new store registers itself in the module-level
        [`registry`][proxystore.store.registry.registry] and is not the
        [`owner`][proxystore.store.base.Store.owner] of the objects stored
        by its connector.

        Args:
            config: Store configuration. If `config` does not contain an
                ID, a new store with a new ID is always made.

        Returns:
            Store instance.
        """
        # base.py imports this module so Store is imported here.
        from proxystore.store.base import Store

        if config.id is None:
            return Store.from_config(config, owner=False)

        with self._lock:
            store = self._stores.get(config.id)
            if store is not None:
                return store
            create_lock = self._create_locks.setdefault(
                config.id,
                threading.Lock(),
            )

        with create_lock:
            # Another thread may have made the store while this one waited.
            store = self.get(config.id)
            if store is not None:
                return store

            new_store = Store.from_config(config, owner=False)
            registered = self.get(config.id)
            if registered is None or registered is new_store:
                return new_store

            # A store with this ID was made directly with the Store
            # constructor while the connector was being built. Use the
            # registered store so every caller gets the same instance.
            new_store.close()
            return registered

    def register(self, store: Store[Any]) -> None:
        """Register a store.

        Note:
            Stores register themselves when created so this does not need
            to be called.

        If a store with the same ID is already registered, that store stays
        registered and `store` is ignored.
        """
        with self._lock:
            if store.id not in self._stores:
                self._stores[store.id] = store
                logger.debug('Registered %r', store)

    def unregister(self, store: Store[Any]) -> None:
        """Unregister a store.

        Note:
            Stores unregister themselves when closed so this does not need
            to be called.

        Nothing happens if a different store with the same ID is
        registered.
        """
        with self._lock:
            if self._stores.get(store.id) is store:
                del self._stores[store.id]
                logger.debug('Unregistered %r', store)


registry = StoreRegistry()
"""Registry of the stores in this process."""
