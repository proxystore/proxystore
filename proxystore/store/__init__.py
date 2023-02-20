"""Module containing all [`Store`][proxystore.store.base.Store] implementations.

| Type | Use Case |
| ---- | -------- |
| [`LocalStore`][proxystore.store.local.LocalStore] | In-memory object store local to the process. Useful for development. |
| [`RedisStore`][proxystore.store.redis.RedisStore] | Store objects in a preconfigured Redis server. |
| [`FileStore`][proxystore.store.file.FileStore] | Use a globally accessible file system for string objects. |
| [`GlobusStore`][proxystore.store.globus.GlobusStore] | Transfer objects between two Globus endpoints. |
| [`EndpointStore`][proxystore.store.endpoint.EndpointStore] | *Experimental:* P2P object stores for multi-site applications. |
| [`MargoStore`][proxystore.store.dim.margo.MargoStore] | *Experimental:* Distributed in-memory storage across nodes with Margo communication. |
| [`UCXStore`][proxystore.store.dim.ucx.UCXStore] | *Experimental:* Distributed in-memory storage across nodes with UCX communication. |
| [`WebsocketStore`][proxystore.store.dim.websockets.WebsocketStore] | *Experimental:* Distributed in-memory storage across nodes with Websocket communication. |
"""  # noqa: E501
from __future__ import annotations

import logging
from typing import Any
from typing import TypeVar

from proxystore.proxy import Proxy
from proxystore.store.base import Store
from proxystore.store.base import StoreFactory
from proxystore.store.exceptions import ProxyStoreFactoryError
from proxystore.store.exceptions import StoreExistsError

T = TypeVar('T')

_stores: dict[str, Store[Any]] = {}
logger = logging.getLogger(__name__)


def get_store(val: str | Proxy[T]) -> Store[Any] | None:
    """Get the backend store with name.

    Args:
        val: name of the store to get or a [`Proxy`][proxystore.proxy.Proxy]
            instance.

    Returns:
        [`Store`][proxystore.store.base.Store] if a store matching the \
        name or belonging to the proxy exists. If the store does not exist, \
        returns `None`.

    Raises:
        ProxyStoreFactoryError: If the value is a proxy but does not contain a
            factory of type
            [`StoreFactory`][proxystore.store.base.StoreFactory].
    """
    if isinstance(val, Proxy):
        # If the object is a proxy, get the factory that will access the store
        factory = val.__factory__
        if isinstance(factory, StoreFactory):
            return factory.get_store()
        else:
            raise ProxyStoreFactoryError(
                'The proxy must contain a factory with type '
                f'{type(StoreFactory).__name__}. {type(factory).__name__} '
                'is not supported.',
            )
    else:
        name = val

    if name in _stores:
        return _stores[name]
    return None


def register_store(store: Store[Any], exist_ok: bool = False) -> None:
    """Register the store instance to the global registry.

    Note:
        Global means globally accessible within the Python process.

    Args:
        store: Store instance to register.
        exist_ok: If a store with the same name exists, overwrite it.

    Raises:
        StoreExistsError: If a store with the same name is already registered
            and `exist_ok` is false.
    """
    if store.name in _stores and not exist_ok:
        raise StoreExistsError(f'A store named {store.name} already exists.')

    _stores[store.name] = store
    logger.debug(f'added {store.name} to global registry of stores')


def unregister_store(name: str) -> None:
    """Unregisters the store instance from the global registry.

    Note:
        This function is a no-op if no store matching the name
        exists (i.e., no exception will be raised).

    Args:
        name: Name of the store to unregister.
    """
    if name in _stores:
        del _stores[name]
        logger.debug(f'removed {name} from global registry of stores')
