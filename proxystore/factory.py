"""ProxyStore Factory Implementations

Factories are callable classes that wrap up the functionality needed
to resolve a proxy, where resolving is the process of retrieving the
object from wherever it is stored such that the proxy can act as the
object.
"""
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import proxystore as ps

default_pool = ThreadPoolExecutor()


class BaseFactory:
    """Base Factory

    This class acts as the base class for all factory types and as a simple
    factory that stores an object as an attribute and returns the object
    when called.

    The :class:`Proxy <ps.proxy.Proxy>` constructor requires that all factories
    passed to it be instances of this
    :class:`BaseFactory <.BaseFactory>`. All classes that inherit from
    :class:`BaseFactory <.BaseFactory>` should implement
    :func:`resolve() <proxystore.factory.BaseFactory.resolve()>` and
    :func:`resolve_async() <proxystore.factory.BaseFactory.resolve_async()>`.

    Note:
        If a custom factory is not-pickleable, :func:`__reduce__()` and
        :func:`__reduce_ex__()` may need to be implemented, as in
        :class:`RedisFactory <.RedisFactory>`.
        Writing custom pickling functions is also beneifical to ensure that
        a pickled factory does not contain the object itself, just what is
        needed to resolve the object to keep the final, pickled factory as
        small as possible.

    Args:
        obj: object to be produced by calling this factory.
    """

    def __init__(self, obj: Any) -> None:
        """Init BaseFactory"""
        self.obj = obj

    def __call__(self) -> Any:
        """Resolve object"""
        return self.resolve()

    def resolve(self) -> Any:
        """Return underlying object"""
        return self.obj

    def resolve_async(self) -> None:
        """Asynchronously resolves underlying object

        Note:
            The API has no requirements about the implementation
            details of this method, only that :func:`resolve()` will
            correctly deal with any side-effects of a call to
            :func:`resolve_async()`.
        """
        pass


class KeyFactory(BaseFactory):
    """Factory for LocalBackend

    The :class:`KeyFactory <.KeyFactory>` stores a key, and when called,
    the :class:`KeyFactory <.KeyFactory>` returns the object associated with
    the key in the backend store.

    Args:
        key (str): key associated with object in the backend store that
            the factory will return upon being called.
    """

    def __init__(self, key: str) -> None:
        """Init KeyFactory"""
        self.key = key

    def resolve(self) -> Any:
        """Return object associated with key"""
        return ps.store.get(self.key)


class RedisFactory(KeyFactory):
    """Factory class for objects in Redis

    Extension of :class:`KeyFactory <.KeyFactory>` with support for
    asynchronously retrieving objects from a
    :class:`RedisStore <proxystore.backend.store.RedisStore>` backend and
    optional, strict guarentees on object versions.

    The :class:`RedisFactory <.RedisFactory>` also stores the hostname and
    port of the Redis server so a connection to the Redis server can be
    established if the proxy containing this factory is passed to a different
    process or machine.

    Args:
        key (str): key used to retrive object from Redis.
        hostname (str): hostname of Redis server.
        port (int): port Redis server is listening on.
        serialize (bool): if `True`, object in store is serialized and
            should be deserialized upon retrival (default: `True`).
        strict (bool): if `True`, ensures that the underlying object
            retrieved from the store is the most up to date version.
            Otherwise, an older version of an object associated with `key`
            may be returned if it is cached locally (default: `False`).
    """

    def __init__(
        self,
        key: str,
        hostname: str,
        port: int,
        serialize: bool = True,
        strict: bool = False,
    ) -> None:
        """Init RedisFactory"""
        self.key = key
        self.hostname = hostname
        self.port = port
        self.serialize = serialize
        self.strict = strict
        self.obj_future = None

    def __reduce__(self):
        """Helper method for pickling"""
        return RedisFactory, (
            self.key,
            self.hostname,
            self.port,
            self.serialize,
            self.strict,
        )

    def __reduce_ex__(self, protocol):
        """See `__reduce__`"""
        return self.__reduce__()

    def resolve(self) -> Any:
        """Get object associated with key from Redis"""
        if ps.store is None:
            ps.init_redis_backend(self.hostname, self.port)

        if self.obj_future is not None:
            obj = self.obj_future.result()
            self.obj_future = None
            return obj

        return ps.store.get(
            self.key, deserialize=self.serialize, strict=self.strict
        )

    def resolve_async(self) -> None:
        """Asynchronously get object associated with key from Redis"""
        if ps.store is None:
            ps.init_redis_backend(self.hostname, self.port)

        # If the value is locally cached by the value server, starting up
        # a separate thread to retrieve a cached value will be slower than
        # just getting the value from the cache
        if ps.store.is_cached(self.key, self.strict):
            return

        self.obj_future = default_pool.submit(
            ps.store.get,
            self.key,
            deserialize=self.serialize,
            strict=self.strict,
        )
