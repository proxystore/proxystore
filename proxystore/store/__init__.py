"""The ProxyStore [`Store`][proxystore.store.base.Store] interface."""

from __future__ import annotations

import contextlib
import logging
import threading
from collections.abc import Generator
from collections.abc import Sequence
from typing import Any
from typing import TypeVar

from proxystore.connectors.protocols import Connector
from proxystore.proxy import get_factory
from proxystore.proxy import Proxy
from proxystore.store.base import Store
from proxystore.store.config import StoreConfig
from proxystore.store.exceptions import ProxyStoreFactoryError
from proxystore.store.exceptions import StoreExistsError
from proxystore.store.factory import StoreFactory
from proxystore.store.types import ConnectorT

__all__ = [
    'Store',
    'StoreConfig',
    'StoreFactory',
    'get_store',
    'register_store',
    'store_registration',
    'unregister_store',
]

T = TypeVar('T')

_stores: dict[str, Store[Any]] = {}
_stores_lock = threading.RLock()
logger = logging.getLogger(__name__)


def get_store(val: str | Proxy[T]) -> Store[Any] | None:
    """Get a registered store by name.

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
        factory = get_factory(val)
        if isinstance(factory, StoreFactory):
            return factory.get_store()
        else:
            raise ProxyStoreFactoryError(
                'The proxy must contain a factory with type '
                f'{StoreFactory.__name__}. {type(factory).__name__} '
                'is not supported.',
            )
    else:
        name = val

    with _stores_lock:
        if name in _stores:
            return _stores[name]
        return None


def get_or_create_store(
    store_config: StoreConfig,
    *,
    register: bool = True,
) -> Store[Any]:
    """Get a registered store or initialize a new instance from the config.

    Args:
        store_config: Store configuration used to reinitialize the store if
            needed.
        register: Optionally register the store if a new instance was
            initialized.

    Returns:
        [`Store`][proxystore.store.base.Store] instance.
    """
    with _stores_lock:
        store = get_store(store_config.name)
        if store is None:
            store = Store.from_config(store_config)
            if register:
                # Set exists_ok here because the store may have initialized
                # itself if register=True.
                register_store(store, exist_ok=True)
        return store


def register_store(store: Store[Any], exist_ok: bool = False) -> None:
    """Register the store instance to the global registry.

    Note:
        Global means globally accessible within the Python process.

    Tip:
        Use the [`store_registration`][proxystore.store.store_registration]
        context manager to automatically register and unregister as store.

    Args:
        store: Store instance to register.
        exist_ok: If a store with the same name exists, overwrite it.

    Raises:
        StoreExistsError: If a store with the same name is already registered
            and `exist_ok` is false.
    """
    with _stores_lock:
        if store.name in _stores and not exist_ok:
            raise StoreExistsError(
                f'A store named "{store.name}" already exists.',
            )

        _stores[store.name] = store
        logger.info(f'Registered a store named "{store.name}"')


@contextlib.contextmanager
def store_registration(
    *stores: Store[Any],
    exist_ok: bool = False,
) -> Generator[None, None, None]:
    """Context manager that registers and unregisters a set of stores.

    Example:
        ```python
        from proxystore.connectors.local import LocalConnector
        from proxystore.store import Store
        from proxystore.store import store_registration

        with Store('store', LocalConnector()) as store:
            with store_registration(store):
                ...

        stores = [
            Store('store1', LocalConnector()),
            Store('store2', LocalConnector()),
        ]
        with store_registration(*stores):
            ...
        ```

    Args:
        stores: Set of [`Store`][proxystore.store.base.Store] instances to
            register then unregister when the context manager is exited.
        exist_ok: If a store with the same name exists, overwrite it.

    Raises:
        StoreExistsError: If a store with the same name is already registered
            and `exist_ok` is false.
    """
    for store in stores:
        register_store(store, exist_ok=exist_ok)

    yield

    for store in stores:
        unregister_store(store)


def unregister_store(name_or_store: str | Store[Any]) -> None:
    """Unregisters the store instance from the global registry.

    Note:
        This function is a no-op if no store matching the name
        exists (i.e., no exception will be raised).

    Args:
        name_or_store: Name of the store to unregister or a store itself.
    """
    name = (
        name_or_store if isinstance(name_or_store, str) else name_or_store.name
    )
    with _stores_lock:
        if name in _stores:
            del _stores[name]
            logger.info(f'Unregistered a store named {name}')
