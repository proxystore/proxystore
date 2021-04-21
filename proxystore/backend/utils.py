"""Utilities for Initializing Backends"""
import proxystore as ps
import proxystore.backend.store as store


def init_local_backend() -> None:
    """Initialize local key-value store"""
    if ps.store is not None:
        if isinstance(ps.store, store.LocalStore):
            return
        raise ValueError(
            'Backend is already initialized to {}. '
            'ProxyStore does not support using multiple backends '
            'at the same time.'.format(type(ps.store))
        )

    ps.store = store.LocalStore()


def init_redis_backend(hostname: str, port: int) -> None:
    """Initialize a Redis client as the global backend key-value store

    Args:
        hostname (str): Redis server hostname
        port (int): Redis server port
    """
    if ps.store is not None:
        if isinstance(ps.store, store.RedisStore):
            return
        raise ValueError(
            'Backend is already initialized to {}. '
            'ProxyStore does not support using multiple backends '
            'at the same time.'.format(type(ps.store))
        )

    ps.store = store.RedisStore(hostname, port)
