"""Utilities for Initializing Backends"""
import proxystore as ps
import proxystore.backend.store as store


def init_local_backend() -> None:
    """Initialize local memory backend

    Initializes :data:`ps.store` to an instance of
    :class:`LocalStore <proxystore.backend.store.LocalStore>`. This stores
    objects in the local memory of the process (and therefore can not be used
    to send objects between separate processes).

    Note:
        If a local memory backend has already been initialized, this function
        will return, i.e., the backend will not be reset.

    Raises:
        ValueError:
            if a backend has alread been initialized to a backend type different
            from :class:`LocalStore <proxystore.backend.store.LocalStore>`.
    """
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
    """Initialize a Redis backend

    Initializes :data:`ps.store` to an instance of
    :class:`RedisStore <proxystore.backend.store.RedisStore>`. Objects are
    serialized and stored in Redis.

    Note:
        This function will not launch a Redis server. The user must start
        the Redis server via the
        `redis-server <https://redis.io/topics/quickstart>`_ command line
        utility.

    Note:
        If a Redis backend has already been initialized, this function will
        return, i.e., the backend will not be reset.

    Args:
        hostname (str): Redis server hostname.
        port (int): Redis server port.

    Raises:
        ValueError:
            if a backend has alread been initialized to a backend type
            different from
            :class:`RedisStore <proxystore.backend.store.RedisStore>`.
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
