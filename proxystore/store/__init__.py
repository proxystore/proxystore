"""Module containing all Store implementations."""
from __future__ import annotations

import logging
from enum import Enum
from typing import Any
from typing import TypeVar

from proxystore.proxy import Proxy
from proxystore.store.base import Store as _Store
from proxystore.store.base import StoreFactory
from proxystore.store.endpoint import EndpointStore as _EndpointStore
from proxystore.store.file import FileStore as _FileStore
from proxystore.store.globus import GlobusStore as _GlobusStore
from proxystore.store.local import LocalStore as _LocalStore
from proxystore.store.redis import RedisStore as _RedisStore

T = TypeVar('T')

_stores: dict[str, _Store] = {}
logger = logging.getLogger(__name__)


class StoreError(Exception):
    """Base exception class for store errors."""

    pass


class StoreExistsError(StoreError):
    """Exception raised when a store with the same name already exists."""

    pass


class UnknownStoreError(StoreError):
    """Exception raised when the type of store to initialize is unknown."""

    pass


class ProxyStoreFactoryError(StoreError):
    """Exception raised when a proxy was not created by a Store."""

    pass


class STORES(Enum):
    """Available Store implementations."""

    ENDPOINT = _EndpointStore
    FILE = _FileStore
    GLOBUS = _GlobusStore
    LOCAL = _LocalStore
    REDIS = _RedisStore

    @classmethod
    def get_str_by_type(cls, store: type[_Store]) -> str:
        """Get str corresponding to enum type of a store type.

        Args:
            store: type of store to check enum for

        Returns:
            String that will index :class:`STORES` and return the same type
            as `store`.

        Raises:
            KeyError:
                if enum type matching `store` is not found.
        """
        for option in cls:
            if option.value == store:
                return option.name
        raise KeyError(f'Enum type matching type {store} not found')


def get_store(val: str | Proxy[T]) -> _Store | None:
    """Get the backend store with name.

    Args:
        val: name (str) of the store to get or a
            :any:`Proxy <proxystore.proxy.Proxy>` instance.

    Returns:
        :any:`Store <proxystore.store.base.Store>` if a store matching the
        name or belonging to the proxy exists. If the store does not exist,
        returns `None`.

    Raises:
        ProxyStoreFactoryError:
            if the value is a proxy but does not contain a factory
            of type :any:`StoreFactory <proxystore.store.base.StoreFactory>`.
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


def init_store(
    store_type: str | STORES | type[_Store],
    name: str,
    **kwargs: Any,
) -> _Store:
    """Initialize a backend store and register globally.

    Usage:
        >>> from proxystore.store import init_store
        >>> from proxystore.store import STORES
        >>> from proxystore.store.redis import RedisStore
        >>>
        >>> # The following are equivalent
        >>> init_store('redis', name='default-store', ...)
        >>> init_store(STORES.REDIS, name='default-store', ...)
        >>> init_store(RedisStore, name='default-store', ...)

    Args:
        store_type (str, STORES, Store): type of store to initialize. Can be
            either a string corresponding to an enum value or the enum value
            itself in :class:`STORES <.STORES>` or a subclass of
            :class:`Store <proxystore.store.base.Store>`.
        name (str): unique name of store. The name is needed to get the store
            again with :func:`get_store() <.get_store>`.
        kwargs (dict): keyword args to pass to store constructor.

    Returns:
        :any:`Store <proxystore.store.base.Store>`

    Raises:
        UnknownStoreError:
            if `store_type` is a string but does not match a value
            in :class:`STORES <.STORES>`.
        UnknownStoreError:
            if `store_type` is not a `str`, member of
            :class:`STORES <.STORES>`, or a type that is a subtype of
            :class:`Store <proxystore.store.base.Store>`.
    """
    if isinstance(store_type, str):
        try:
            store = STORES[store_type.upper()].value(name, **kwargs)
        except KeyError:
            raise UnknownStoreError(
                f'No store with name {store_type}. Valid types include: '
                f'{",".join(s.name for s in STORES)}.',
            )
    elif isinstance(store_type, STORES):
        store = store_type.value(name, **kwargs)
    elif issubclass(store_type, _Store):
        store = store_type(name, **kwargs)
    else:
        raise UnknownStoreError(
            'The store_type argument must be a string corresponding to '
            'proxystore.store.STORES, member of proxystore.store.STORES, or '
            'a type that extends Store. '
            f'Found type f{type(store_type).__name__} instead.',
        )

    register_store(store, exist_ok=True)

    return store


def register_store(store: _Store, exist_ok: bool = False) -> None:
    """Register the store instance to the global registry.

    Note:
        Global means globally accessible within the Python process.

    Args:
        store (Store): store instance to register.
        exist_ok (bool): if a store with the same name exists, overwrite it.

    Raises:
        StoreExistsError:
            if a store with the same name is already registered and
            exist_ok is false.
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
        name (str): name of the store to unregister.
    """
    if name in _stores:
        del _stores[name]
        logger.debug(f'removed {name} from global registry of stores')
