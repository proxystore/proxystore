"""Backend Key-Value Store Implementations

The backend object is stored in :attr:`proxystore.store` and can be set
manually; however, it is recommended to use the included initialization
functions. E.g.,

>>> import proxystore as ps
>>> ps.init_redis_backend('localhost', 12345)
"""
import os
import time

from typing import Any, Dict, Optional

try:
    import redis
except ImportError as e:  # pragma: no cover
    # We do not want to raise this ImportError if the user never
    # uses the RedisStore so we delay raising the error until the
    # constructor of RedisStore
    redis = e

from proxystore.backend.serialize import serialize as serialize_obj
from proxystore.backend.serialize import deserialize as deserialize_str
from proxystore.backend.cache import LRUCache

PROXYSTORE_CACHE_SIZE_ENV = 'PROXYSTORE_CACHE_SIZE'
"""Environment variable name for specifying the cache size"""

# Initialize local Python process cache
_cache_size = 16
_env_cache_size = os.environ.get(PROXYSTORE_CACHE_SIZE_ENV, None)
if _env_cache_size is not None:  # pragma: no cover
    try:
        _cache_size = int(_env_cache_size)
    except ValueError:
        raise ValueError(f'Cannot parse {PROXYSTORE_CACHE_SIZE_ENV}='
                         f'{env_cache_size} as integer')
if _cache_size < 0:  # pragma: no cover
    raise ValueError('Cache size cannot be negative')
_cache = LRUCache(_cache_size)



class Store():
    """ProxyStore backend store abstract class"""

    def evict(self, key: str) -> None:
        """Evict object associated with key"""
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        """Check if key exists"""
        raise NotImplementedError

    def get(self, key: str, strict: bool = False) -> Optional[object]:
        """Return object associated with key

        Args:
            key (str): key corresponding to object.
            strict (bool): if `True`, guarentee returned object is the most
                recent version (default: `False`).

        Returns:
            object associated with key or `None` if key does not exist
        """
        raise NotImplementedError

    def is_cached(self, key: str, strict: bool = False) -> bool:
        """Check if object is cached locally

        Args:
            key (str): key corresponding to object.
            strict (bool): if `True`, guarentee that cached object is the most.
                recent version (default: `False`).
        """

    def set(self, key: str, obj: Any) -> None:
        """Set key-object pair in store"""
        raise NotImplementedError


class LocalStore(Store):
    """Local Memory Key-Object Store"""

    def __init__(self) -> None:
        """Init Store"""
        self.store = {}

    def evict(self, key: str) -> None:
        """Evict object associated with key"""
        if key in self.store:
            del self.store[key]

    def exists(self, key: str) -> bool:
        """Check if key exists"""
        return key in self.store

    def get(self, key: str, strict: bool = False) -> Optional[object]:
        """Return object associated with key

        Args:
            key (str): key corresponding to object.
            strict (bool): if `True`, guarentee returned object is the most
                recent version (default: `False`).

        Returns:
            object associated with key or `None` if key does not exist
        """
        if key in self.store:
            return self.store[key]
        return None

    def is_cached(self, key: str, strict: bool = False) -> bool:
        """Check if object is cached locally

        Args:
            key (str): key corresponding to object.
            strict (bool): if `True`, guarentee that cached object is the most.
                recent version (default: `False`).
        """
        return key in self.store

    def set(self, key: str, obj: Any) -> None:
        """Set key-object pair in store"""
        self.store[key] = obj


class RemoteStore(Store):
    """Base class for interfacing with remove key-value stores

    Provides base functionality for interaction with a remote store.

    Classes extending :class:`RemoteStore` must implement :func:`evict()`,
    :func:`exists()`, :func:`get_str()` and :func:`set_str()`. The
    :class:`RemoteStore` handles the caching. The cache stores
    :data:`key: (timestamp, obj)` pairs.

    :class:`RemoteStore` stores key-string pairs, i.e., objects passed to
    :func:`get()` or :func:`set()` will be appropriately (de)serialized.
    Functionality for serialized, caching, and strict guarentees are already
    provided in :func:`get()` and :func:`set()`.

    The local (per-Python process) cache size can be overridden by setting the
    environment variable defined in
    :data:`proxystore.backend.store.PROXYSTORE_CACHE_SIZE_ENV`.
    """

    def __init__(self) -> None:
        """Init RemoteStore"""
        pass

    def evict(self, key: str) -> None:
        """Evict object associated with key from remote store"""
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        """Check if key exists in remote store"""
        raise NotImplementedError

    def get_str(self, key: str) -> Optional[str]:
        """Get serialized object from remote store"""
        raise NotImplementedError

    def set_str(self, key: str, data: str) -> None:
        """Set serialized object in remote store with key"""
        raise NotImplementedError

    def get(
        self, key: str, deserialize: bool = True, strict: bool = False
    ) -> Optional[object]:
        """Return object associated with key

        Args:
            key (str): key corresponding to object.
            deserialize (bool): deserialize object if `True`. If objects
                are custom serialized, set this as `False` (default: `True`).
            strict (bool): if `True`, guarentee returned object is the most
                recent version (default: `False`).

        Returns:
            object associated with key or `None` if key does not exist.
        """
        if self.is_cached(key, strict):
            return _cache.get(key)[1]

        value = self.get_str(key)
        if value is not None:
            timestamp = float(self.get_str(key + '_timestamp'))
            if deserialize:
                value = deserialize_str(value)
            _cache.set(key, (timestamp, value))
            return value

        return None

    def is_cached(self, key: str, strict: bool = False) -> bool:
        """Check if object is cached locally

        Args:
            key (str): key corresponding to object
            strict (bool): if `True`, guarentee that cached object is the most.
                recent version (default: `False`).
        """
        if _cache.exists(key):
            if strict:
                store_timestamp = float(self.get_str(key + '_timestamp'))
                cache_timestamp = _cache.get(key)[0]
                return cache_timestamp >= store_timestamp
            return True
        return False

    def set(self, key: str, obj: Any, serialize: bool = True) -> None:
        """Set key-object pair in store

        Serializes `obj` to a string and then calls :func:`set_str(obj_str)`.

        Args:
            key (str): key to associate with object in store.
            obj (object): object place in store.
            serialize (bool): serialize object before placing in store. If
                `obj` is already serialized, set as `False` (default: True).
        """
        if serialize:
            obj = serialize_obj(obj)

        self.set_str(key, obj)
        self.set_str(key + '_timestamp', str(time.time()))


class RedisStore(RemoteStore):
    """Redis backend class

    Args:
        hostname (str): Redis server hostname.
        port (int): Redis server port.

    Raise:
        ImportError:
            if `redis-py <https://redis-py.readthedocs.io/en/stable/>`_
            is not installed.
    """

    def __init__(self, hostname: str, port: int) -> None:
        """Init RedisStore"""
        if isinstance(redis, ImportError):  # pragma: no cover
            raise ImportError(
                'The redis-py package must be installed to use the '
                'RedisStore backend'
            )

        self.hostname = hostname
        self.port = port
        self.redis_client = redis.StrictRedis(
            host=hostname, port=port, decode_responses=True
        )
        super(RedisStore, self).__init__()

    def evict(self, key: str) -> None:
        """Evict object associated with key from remote store"""
        self.redis_client.delete(key)

    def exists(self, key: str) -> bool:
        """Check if key exists in remote store"""
        return self.redis_client.exists(key)

    def get_str(self, key: str) -> Optional[str]:
        """Get serialized object from remote store"""
        return self.redis_client.get(key)

    def set_str(self, key: str, data: str) -> None:
        """Set serialized object in remote store with key"""
        self.redis_client.set(key, data)
