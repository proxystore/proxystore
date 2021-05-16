from proxystore.store.base import Store as _Store
from proxystore.store.local import LocalStore as _LocalStore
from proxystore.store.redis import RedisStore as _RedisStore

__all__ = ['get_store', 'init_store']

_STORE_NAME_TO_TYPE = {
    'local': _LocalStore,
    'redis': _RedisStore,
}

_stores = {}


def get_store(name: str) -> _Store:
    """Get the backend store with name

    Args:
        name (str): name of store to get.

    Returns:
        :any:`Store <proxystore.store.base.Store>` if store with `name` exists
        else `None`.
    """
    if name in _stores:
        return _stores[name]
    return None


def init_store(name: str, *args, **kwargs) -> _Store:
    """Initializes a backend store

    Note:
        If a store of the same name has already been initialized, the current
        store will be replaced with a new. This is because the store parameters
        may have changed.

    Args:
        name (str): name of store to initialize.
        args (list): args to pass to store constructor.
        kwargs (dict): keyword args to pass to store constructor.

    Returns:
        :any:`Store <proxystore.store.base.Store>`

    Raises:
        ValueError:
            if a store corresponding to `name` is not found.
    """
    if name in _STORE_NAME_TO_TYPE:
        store_class = _STORE_NAME_TO_TYPE[name]
    else:
        raise ValueError(f'No store with name {name}')

    _stores[name] = store_class(*args, **kwargs)

    return _stores[name]
