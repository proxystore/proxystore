import os
import pickle as pkl
import redis
import time

from typing import Any, Dict, Optional

from proxystore.backend import PROXYSTORE_CACHE_SIZE_ENV
from proxystore.backend import serialize, deserialize
from proxystore.backend.cache import LRUCache


class BaseStore():
    """Backend Store Abstract Class"""
    def evict(self, key: str) -> None:
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        raise NotImplementedError

    def get(self, key: str, strict: bool = False) -> Optional[object]:
        raise NotImplementedError
    
    def is_cached(self, key: str, strict: bool = False) -> bool:
        raise NotImplementedError

    def set(self, key: str, obj: Any) -> None:
        raise NotImplementedError


class LocalStore(BaseStore):
    """Local Memory Store"""
    def __init__(self) -> None:
        self.store = {}

    def evict(self, key: str) -> None:
        if key in self.store:
            del self.store[key]

    def exists(self, key: str) -> bool:
        return key in self.store

    def get(self, key: str, strict: bool = False) -> Optional[object]:
        if key in self.store:
            return self.store[key]
        return None
    
    def is_cached(self, key: str, strict: bool = False) -> bool:
        return key in self.store

    def set(self, key: str, obj: Any) -> None:
        self.store[key] = obj


class CachedStore(BaseStore):
    """Base class for backends with caching support

    Classes extending `BaseStore` must implement `evict()`, `exists()`
    `get_str()` and `set_str()`. The BaseStore handles the cache.
    The cache stores key: (timestamp, obj) pairs.
    """
    def __init__(self, cache_size: int = 16) -> None:
        """
        Args:
            cache_size (int): number of objects cache can hold
        """
        if cache_size < 0:
            raise ValueError('Cache size cannot be negative')
        env_cache_size = os.environ.get(PROXYSTORE_CACHE_SIZE_ENV, None)
        if env_cache_size is not None:
            cache_size = int(env_cache_size)
        self._cache = LRUCache(cache_size) if cache_size > 0 else None

    def evict(self, key: str) -> None:
        """Evict value corresponding to `key` from store"""
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        raise NotImplementedError

    def get_str(self, key: str) -> Optional[str]:
        raise NotImplementedError

    def set_str(self, key: str, data: str) -> None:
        raise NotImplementedError

    def get(self, key: str, strict: bool = False) -> Optional[object]:
        if self.is_cached(key, strict):
            return self._cache.get(key)[1]

        value = self.get_str(key)
        if value is not None:
            timestamp = float(self.get_str(key + '_timestamp'))
            obj = deserialize(value)
            if self._cache is not None:
               self._cache.set(key, (timestamp, obj))
            return obj

        return None
    
    def is_cached(self, key: str, strict: bool = False) -> bool:
        if self._cache is None:
            return False

        if self._cache.exists(key):
            if strict:
                store_timestamp = float(self.get_str(key + '_timestamp'))
                cache_timestamp = self._cache.get(key)[0]
                return cache_timestamp >= store_timestamp
            return True
        return False

    def set(self, key: str, obj: Any) -> None:
        obj = serialize(obj)

        self.set_str(key, obj)
        self.set_str(key + '_timestamp', str(time.time()))


class RedisStore(CachedStore):
    """Redis backend class"""
    def __init__(self,
                 hostname: str,
                 port: int,
                 **kwargs: Dict[str, Any]
    ) -> None:
        """
        Args:
            hostname (str): Redis server hostname
            port (int): Redis server port
        """
        self.hostname = hostname
        self.port = port
        self.redis_client = redis.StrictRedis(host=hostname, port=port,
                                              decode_responses=True)
        super(RedisStore, self).__init__(**kwargs)

    def evict(self, key: str) -> None:
        """Evict value corresponding to key from Redis"""
        self.redis_client.delete(key)

    def exists(self, key: str) -> bool:
        """Check if key exists in Redis"""
        return self.redis_client.exists(key)

    def get_str(self, key: str) -> Optional[str]:
        """Get string associated with key from Redis"""
        return self.redis_client.get(key)

    def set_str(self, key: str, data: str) -> None:
        """Set `key` to `data` in Redis"""
        self.redis_client.set(key, data)
