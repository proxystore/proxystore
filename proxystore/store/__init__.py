from enum import Enum as _Enum
from typing import Union as _Union

from proxystore.store.base import Store as _Store
from proxystore.store.local import LocalStore as _LocalStore
from proxystore.store.redis import RedisStore as _RedisStore

__all__ = ['get_store', 'init_store']

_stores = {}


class STORES(_Enum):
    """Store options"""

    LOCAL = _LocalStore
    REDIS = _RedisStore


def get_store(name: str) -> _Store:
    """Get the backend store with name

    Args:
        name (str): name store to get.

    Returns:
        :any:`Store <proxystore.store.base.Store>` if store with `name` exists
        else `None`.
    """
    if name in _stores:
        return _stores[name]
    return None


def init_store(store_type: _Union[str, STORES], name: str, **kwargs) -> _Store:
    """Initializes a backend store

    Note:
        If a store of the same name has already been initialized, the current
        store will be replaced with a new. This is because the store parameters
        may have changed.

    Args:
        store_type (str, STORES): type of store to initialize.
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
    """
    if isinstance(store_type, str):
        try:
            store_type = STORES[store_type.upper()]
        except KeyError:
            raise ValueError(f'No store with name {store_type}.')
    elif not isinstance(store_type, STORES):
        raise ValueError(
            'Arg store_type must be str or member of proxystore.store.STORES.'
            f'Found type f{type(store_type)} instead.'
        )

    _stores[name] = store_type.value(name, **kwargs)

    return _stores[name]
