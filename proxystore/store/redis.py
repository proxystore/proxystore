"""RedisStore Implementation"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

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
from proxystore.store.remote import RemoteFactory, RemoteStore


class RedisFactory(RemoteFactory):
    """Factory for Instances of RedisStore

    Adds support for asynchronously retrieving objects from a
    :class:`RedisStore <.RedisStore>` backend and optional, strict guarentees
    on object versions.

    The factory takes the `store_type` and `store_args` parameters that are
    used to reinitialize the backend store if the factory is sent to a remote
    process backend has not already been initialized.
    """

    def __init__(
        self,
        key: str,
        store_name: str,
        store_kwargs: Dict[str, Any] = {},
        *,
        evict: bool = False,
        serialize: bool = True,
        strict: bool = False,
    ) -> None:
        """Init RedisFactory

        Args:
            key (str): key corresponding to object in store.
            store_name (str): name of store.
            store_kwargs (dict): optional keyword arguments used to 
                reinitialize store.
            evict (bool): If True, evict the object from the store once
                :func:`resolve()` is called (default: False).
            serialize (bool): if True, object in store is serialized and
                should be deserialized upon retrival (default: True).
            strict (bool): guarentee object produce when this object is called
                is the most recent version of the object associated with the
                key in the store (default: False).
        """
        super(RedisFactory, self).__init__(
            key,
            RedisStore,
            store_name,
            store_kwargs,
            evict=evict,
            serialize=serialize,
            strict=strict
        )


class RedisStore(RemoteStore):
    """Redis backend class"""

    def __init__(
        self,
        name: str,
        *,
        hostname: str,
        port: int,
        cache_size: int = 16,
    ) -> None:
        """Init RedisStore

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
        *,
        key: Optional[str] = None,
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
                self.set(obj, key=key, serialize=kwargs['serialize'])
            else:
                self.set(obj, key=key)
        elif not self.exists(key):
            raise ValueError(
                f'An object with key {key} does not exist in the store'
            )
        return Proxy(
            factory(
                key,
                store_name=self.name,
                store_kwargs={
                    'hostname': self.hostname,
                    'port': self.port,
                    'cache_size': self.cache_size,
                },
                **kwargs,
            )
        )
