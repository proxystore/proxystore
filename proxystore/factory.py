from concurrent.futures import ThreadPoolExecutor
from typing import Any

import proxystore as ps

default_pool = ThreadPoolExecutor()


class BaseFactory():
    """Base `Factory` class"""
    def __init__(self, obj: Any) -> None:
        self.obj = obj

    def __call__(self) -> Any:
        return self.resolve()

    def resolve(self) -> Any:
        """Returns the underlying object"""
        return self.obj

    def resolve_async(self) -> None:
        """Asynchronously resolves the underlying object

        Note: The API has no requirements about the implementation
            details of this method, only that `resolve()` will
            correctly deal with any side-effects of a call to
            `resolve_async()`.
        """
        pass


class KeyFactory(BaseFactory):
    def __init__(self, key: str) -> None:
        self.key = key

    def resolve(self) -> Any:
        return ps.store.get(self.key)


class RedisFactory(KeyFactory):
    """Factory class for objects in Redis"""
    def __init__(self,
                 key: str,
                 hostname: str,
                 port: int,
                 strict: bool = False) -> None:
        """
        Args:
            key (str): key used to retrive object from Redis
            hostname (str): hostname of Redis server
            port (int): port Redis server is listening on
            strict (bool): if `True`, ensures that the underlying object
                retrieved from the store is the most up to date version.
                Otherwise, an older version of an object associated with `key`
                may be returned if it is cached locally.
        """
        self.key = key
        self.hostname = hostname
        self.port = port
        self.strict = strict
        self.obj_future = None

    def __reduce__(self):
        """Helper method for pickling"""
        return RedisFactory, (self.key, self.hostname, self.port, self.strict,)

    def __reduce_ex__(self, protocol):
        """See `__reduce__`"""
        return self.__reduce__()

    def resolve(self) -> Any:
        if ps.store is None:
            ps.backend.init_redis_backend(self.hostname, self.port)

        if self.obj_future is not None:
            obj = self.obj_future.result()
            self.obj_future = None
            return obj

        return ps.store.get(self.key, strict=self.strict)

    def resolve_async(self) -> None:
        if ps.store is None:
            ps.backend.init_redis_backend(self.hostname, self.port)

        # If the value is locally cached by the value server, starting up
        # a separate thread to retrieve a cached value will be slower than
        # just getting the value from the cache
        if ps.store.is_cached(self.key, self.strict):
            return

        self.obj_future = default_pool.submit(
                ps.store.get, self.key, strict=self.strict)
