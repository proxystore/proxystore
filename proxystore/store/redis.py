"""RedisStore Implementation"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional

try:
    import redis
except ImportError as e:  # pragma: no cover
    # We do not want to raise this ImportError if the user never
    # uses the RedisStore so we delay raising the error until the
    # constructor of RedisStore
    redis = e

import proxystore as ps
from proxystore.factory import Factory
from proxystore.proxy import Proxy
from proxystore.store.base import RemoteStore

_default_pool = ThreadPoolExecutor()


class RedisFactory(Factory):
    """Factory for RedisStore

    Adds support for asynchronously retrieving objects from a
    :class:`RedisStore <.RedisStore>` backend and
    optional, strict guarentees on object versions.

    The :class:`RedisFactory <.RedisFactory>` also stores the hostname and
    port of the Redis server so a connection to the Redis server can be
    established if the proxy containing this factory is passed to a different
    process or machine.
    """

    def __init__(
        self,
        key: str,
        name: str,
        hostname: str,
        port: str,
        *,
        evict: bool = False,
        serialize: bool = True,
        strict: bool = False,
        **kwargs: Dict[str, Any],
    ) -> None:
        """Init RedisFactory

        Args:
            key (str): key corresponding to object in store.
            name (str): name of store to retrieve object from.
            hostname (str): hostname of Redis server.
            port (int): port Redis server.
            evict (bool): If True, evict the object from the store once
                :func:`resolve()` is called (default: False).
            serialize (bool): if True, object in store is serialized and
                should be deserialized upon retrival (default: True).
            strict (bool): guarentee object produce when this object is called
                is the most recent version of the object associated with the
                key in the store (default: False).
            kwargs (dict): additional keyword arguments to pass to
                :class:`RedisStore <.RedisStore>` to rebuild the instance
        """
        self.key = key
        self.name = name
        self.hostname = hostname
        self.port = port
        self.evict = evict
        self.serialize = serialize
        self.strict = strict
        self._kwargs = kwargs
        self._obj_future = None

    def __getnewargs_ex__(self):
        """Helper method for pickling"""
        return (self.key, self.name, self.hostname, self.port), {
            'evict': self.evict,
            'serialize': self.serialize,
            'strict': self.strict,
            **self._kwargs,
        }

    def resolve(self) -> Any:
        """Get object associated with key from Redis"""
        if self._obj_future is not None:
            obj = self._obj_future.result()
            self._obj_future = None
            return obj

        store = ps.store.get_store(self.name)
        if store is None:
            store = ps.store.init_store(
                ps.store.STORES.REDIS,
                self.name,
                hostname=self.hostname,
                port=self.port,
                **self._kwargs,
            )
        obj = store.get(
            self.key, deserialize=self.serialize, strict=self.strict
        )
        if self.evict:
            store.evict(self.key)
        return obj

    def resolve_async(self) -> None:
        """Asynchronously get object associated with key from Redis"""
        store = ps.store.get_store(self.name)
        if store is None:
            store = ps.store.init_store(
                ps.store.STORES.REDIS,
                self.name,
                hostname=self.hostname,
                port=self.port,
                **self._kwargs,
            )

        # If the value is locally cached by the value server, starting up
        # a separate thread to retrieve a cached value will be slower than
        # just getting the value from the cache
        if store.is_cached(self.key, strict=self.strict):
            return

        self._obj_future = _default_pool.submit(
            store.get,
            self.key,
            deserialize=self.serialize,
            strict=self.strict,
        )


class RedisStore(RemoteStore):
    """Redis backend class

    Args:
        name (str): name of the store instance.
        hostname (str): Redis server hostname.
        port (int): Redis server port.
        cache_size (int): size of local cache (in # of objects). If 0,
            the cache is disabled (default: 16).

    Raise:
        ImportError:
            if `redis-py <https://redis-py.readthedocs.io/en/stable/>`_
            is not installed.
    """

    def __init__(
        self,
        name: str,
        hostname: str,
        port: int,
        cache_size: int = 16,
    ) -> None:
        """Init RedisStore"""
        if isinstance(redis, ImportError):  # pragma: no cover
            raise ImportError(
                'The redis-py package must be installed to use the '
                'RedisStore backend'
            )

        self.hostname = hostname
        self.port = port
        self._redis_client = redis.StrictRedis(
            host=hostname, port=port, decode_responses=True
        )
        super(RedisStore, self).__init__(name, cache_size=cache_size)

    def evict(self, key: str) -> None:
        """Evict object associated with key from Redis

        Args:
            key (str): key corresponding to object in store to evict.
        """
        self._redis_client.delete(key)
        self._cache.evict(key)

    def exists(self, key: str) -> bool:
        """Check if key exists in Redis

        Args:
            key (str): key to check.

        Returns:
            `bool`
        """
        return self._redis_client.exists(key)

    def get_str(self, key: str) -> Optional[str]:
        """Get serialized object from Redis

        Args:
            key (str): key corresponding to object.

        Returns:
            serialized object or `None` if it does not exist.
        """
        return self._redis_client.get(key)

    def set_str(self, key: str, data: str) -> None:
        """Set serialized object in Redis with key

        Args:
            key (str): key corresponding to object.
            data (str): serialized object.
        """
        self._redis_client.set(key, data)

    def proxy(
        self,
        obj: Optional[object] = None,
        key: Optional[str] = None,
        *,
        factory: Factory = RedisFactory,
        **kwargs,
    ) -> 'proxystore.proxy.Proxy':  # noqa: F821
        """Create a proxy that will resolve to an object in the store

        Args:
            obj (object): object to place in store and return proxy for.
                If an object is not provided, a key must be provided that
                corresponds to an object already in the store (default: None).
            key (str): optional key to associate with `obj` in the store.
                If not provided, a key will be generated (default: None).
            factory (Factory): factory class that will be instantiated
                and passed to the proxy. The factory class should be able
                to correctly resolve the object from this store
                (default: :class:`RedisFactory <.RedisFactory>`).
            kwargs (dict): additional arguments to pass to the Factory.

        Returns:
            :any:`Proxy <proxystore.proxy.Proxy>`

        Raises:
            ValueError:
                if `key` and `obj` are both `None`.
            ValueError:
                if `obj` is None and `key` does not exist in the store.
        """
        if key is None and obj is None:
            raise ValueError('At least one of key or obj must be specified')
        if key is None:
            key = ps.utils.create_key(obj)
        if obj is not None:
            if 'serialize' in kwargs:
                self.set(key, obj, serialize=kwargs['serialize'])
            else:
                self.set(key, obj)
        elif not self.exists(key):
            raise ValueError(
                f'An object with key {key} does not exist in the store'
            )
        return Proxy(
            factory(
                key,
                self.name,
                self.hostname,
                self.port,
                cache_size=self.cache_size,
                **kwargs,
            )
        )
