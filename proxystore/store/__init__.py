"""The ProxyStore [`Store`][proxystore.store.base.Store] interface.

Every [`Store`][proxystore.store.base.Store] is registered in a registry
that is global to the Python process and keyed by the unique
[`id`][proxystore.store.base.Store.id] of the store. A store is registered
when it is created and unregistered when it is closed. When a proxy is
resolved, the store which created the proxy is looked up in the registry,
or initialized and registered from the configuration contained in the proxy
if the store does not exist in the process.
"""

from __future__ import annotations

import logging
import threading
from typing import Any
from typing import TypeVar

from proxystore.proxy import get_factory
from proxystore.proxy import Proxy
from proxystore.store.base import Store
from proxystore.store.config import StoreConfig
from proxystore.store.exceptions import ProxyStoreFactoryError
from proxystore.store.factory import StoreFactory

__all__ = [
    'Store',
    'StoreConfig',
    'StoreFactory',
    'get_or_create_store',
    'get_store',
]

T = TypeVar('T')

_stores: dict[str, Store[Any]] = {}
_stores_lock = threading.RLock()
logger = logging.getLogger(__name__)


def get_store(proxy: Proxy[T]) -> Store[Any]:
    """Get the store which created a proxy.

    The store is initialized and registered from the configuration
    contained in the proxy's factory if the store does not already exist
    in this process.

    Args:
        proxy: [`Proxy`][proxystore.proxy.Proxy] instance created by a
            [`Store`][proxystore.store.base.Store].

    Returns:
        [`Store`][proxystore.store.base.Store] which created the proxy.

    Raises:
        ProxyStoreFactoryError: If the proxy does not contain a factory of
            type [`StoreFactory`][proxystore.store.factory.StoreFactory].
    """
    factory = get_factory(proxy)
    if isinstance(factory, StoreFactory):
        return factory.get_store()
    raise ProxyStoreFactoryError(
        'The proxy must contain a factory with type '
        f'{StoreFactory.__name__}. {type(factory).__name__} '
        'is not supported.',
    )


def get_or_create_store(store_config: StoreConfig) -> Store[Any]:
    """Get a registered store or initialize a new instance from the config.

    Note:
        A new store is not the
        [`owner`][proxystore.store.base.Store.owner] of the objects stored
        by its connector, so closing the store does not clear the connector.

    Args:
        store_config: Store configuration. If a store with the same
            [`id`][proxystore.store.config.StoreConfig] is registered, that
            store is returned. Otherwise, a new store is initialized (and
            registered) from the configuration.

    Returns:
        [`Store`][proxystore.store.base.Store] instance.
    """
    with _stores_lock:
        if store_config.id is not None and store_config.id in _stores:
            return _stores[store_config.id]
        return Store.from_config(store_config, owner=False)


def _register_store(store: Store[Any]) -> None:
    # Only stores created from the same configuration share an ID. If a
    # store with the same ID is already registered, that store continues to
    # be used.
    with _stores_lock:
        if store.id not in _stores:
            _stores[store.id] = store
            logger.debug('Registered %r', store)


def _unregister_store(store: Store[Any]) -> None:
    # Only unregister the store if this instance is the registered store.
    with _stores_lock:
        if _stores.get(store.id) is store:
            del _stores[store.id]
            logger.debug('Unregistered %r', store)
