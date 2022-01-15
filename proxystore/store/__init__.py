"""Module containing all Store implementations."""
import logging
from enum import Enum
from typing import Dict
from typing import Optional
from typing import Type
from typing import Union

from proxystore.store.base import Store as _Store
from proxystore.store.file import FileStore as _FileStore
from proxystore.store.globus import GlobusStore as _GlobusStore
from proxystore.store.local import LocalStore as _LocalStore
from proxystore.store.redis import RedisStore as _RedisStore

_stores: Dict[str, _Store] = {}
logger = logging.getLogger(__name__)


class STORES(Enum):
    """Available Store implementations."""

    GLOBUS = _GlobusStore
    LOCAL = _LocalStore
    REDIS = _RedisStore
    FILE = _FileStore

    @classmethod
    def get_str_by_type(cls, store: Type[_Store]) -> str:
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
        raise KeyError(f"Enum type matching type {store} not found")


def get_store(name: str) -> Optional[_Store]:
    """Get the backend store with name.

    Args:
        name (str): name store to get.

    Returns:
        :any:`Store <proxystore.store.base.Store>` if store with `name` exists
        else `None`.
    """
    if name in _stores:
        return _stores[name]
    return None


def init_store(
    store_type: Union[str, STORES, Type[_Store]],
    name: str,
    **kwargs,
) -> _Store:
    """Initialize a backend store and register globally.

    Note:
        If a store of the same name has already been initialized, the current
        store will be replaced with a new. This is because the store parameters
        may have changed.

    Usage:
        >>> import proxystore as ps
        >>>
        >>> # The following are equivalent
        >>> ps.store.init_store('redis', name='default-store', ...)
        >>> ps.store.init_store(ps.store.STORES.REDIS, name='default-store', ...)
        >>> ps.store.init_store(ps.store.redis.RedisStore, name='default-store', ...)

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
        ValueError:
            if a store corresponding to `store_type` is not found.
        ValueError:
            if `store_type` is not a `str` or member of
            :class:`STORES <.STORES>`.
    """  # noqa: E501
    if isinstance(store_type, str):
        try:
            _stores[name] = STORES[store_type.upper()].value(name, **kwargs)
        except KeyError:
            raise ValueError(f"No store with name {store_type}.")
    elif isinstance(store_type, STORES):
        _stores[name] = store_type.value(name, **kwargs)
    elif issubclass(store_type, _Store):
        _stores[name] = store_type(name, **kwargs)
    else:
        raise ValueError(
            "Arg store_type must be str corresponding to "
            "proxystore.store.STORES, member of proxystore.store.STORES, or "
            f"subclass of Store. Found type f{type(store_type)} instead.",
        )

    logger.debug(f"Added {_stores[name]} to globally accessible stores")

    return _stores[name]
