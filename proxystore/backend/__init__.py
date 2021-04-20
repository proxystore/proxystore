import proxystore as ps

from proxystore.backend.serialization import Serializer

PROXYSTORE_CACHE_SIZE_ENV = 'PROXYSTORE_CACHE_SIZE'

serialize = Serializer.serialize
deserialize = Serializer.deserialize


def init_local_backend() -> None:
    """Initialize local key-value store"""
    if ps.store is not None:
        if isinstance(ps.store, ps.backend.store.LocalStore):
            return
        raise ValueError('Backend is already initialized to {}. '
                         'ProxyStore does not support using multiple backends '
                         'at the same time.'.format(type(ps.store)))
    
    ps.store = ps.backend.store.LocalStore()


def init_redis_backend(hostname: str, port: int) -> None:
    """Initialize a Redis client as the global backend key-value store

    Args:
        hostname (str): Redis server hostname
        port (int): Redis server port
    """
    if ps.store is not None:
        if isinstance(ps.store, ps.backend.store.RedisStore):
            return
        raise ValueError('Backend is already initialized to {}. '
                         'ProxyStore does not support using multiple backends '
                         'at the same time.'.format(type(ps.store)))
    
    ps.store = ps.backend.store.RedisStore(hostname, port)

