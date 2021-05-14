"""ProxyStore Factory Implementations

Factories are callable classes that wrap up the functionality needed
to resolve a proxy, where resolving is the process of retrieving the
object from wherever it is stored such that the proxy can act as the
object.
"""
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

import proxystore as ps

default_pool = ThreadPoolExecutor()


class Factory:
    """Abstract Factory Class

    A factory is a callable object that when called, returns an object.
    The :class:`Proxy <ps.proxy.Proxy>` constructor takes an instance of a
    factory and calls the factory when the proxy does its just-in-time
    resolution.

    Note:
        All factory implementations must subclass :class:`Factory <.Factory>`.

    Note:
        If a custom factory is not-pickleable,
        :func:`__getnewargs_ex__` may need to be implemented, as in
        :class:`RedisFactory <.RedisFactory>`.
        Writing custom pickling functions is also beneifical to ensure that
        a pickled factory does not contain the object itself, just what is
        needed to resolve the object to keep the final, pickled factory as
        small as possible.

    Args:
        obj (object): optional object that the factory will resolve. If
            specified, i.e., not None, the Factory constructor should place
            the object in the backend store.
        **kwargs (dict): additional keyword arguments for the Factory.
    """

    def __init__(self, obj: Optional[object] = None, **kwargs) -> None:
        """Init Factory"""
        raise NotImplementedError

    def __call__(self) -> Any:
        """Resolve object"""
        return self.resolve()

    def resolve(self) -> Any:
        """Return underlying object"""
        raise NotImplementedError

    def resolve_async(self) -> None:
        """Asynchronously resolves underlying object

        Note:
            The API has no requirements about the implementation
            details of this method, only that :func:`resolve()` will
            correctly deal with any side-effects of a call to
            :func:`resolve_async()`.
        """
        pass


class SimpleFactory(Factory):
    """Simple Factory that stores object as class attribute

    Args:
        obj (object): object to produce when factory is called.
    """

    def __init__(self, obj: Any) -> None:
        """Init Factory"""
        self.obj = obj

    def __call__(self) -> Any:
        """Resolve object"""
        return self.resolve()

    def resolve(self) -> Any:
        """Return underlying object"""
        return self.obj

    def resolve_async(self) -> None:
        """Asynchronously resolves underlying object"""
        pass


class LocalFactory(Factory):
    """Factory for LocalStore

    The :class:`LocalFactory <.LocalFactory>` stores a key, and when called,
    the :class:`LocalFactory <.LocalFactory>` returns the object associated with
    the key in the backend store.

    Args:
        obj (object): optionally pass the object that this factory will
            resolve. If passed, the object will be placed in the store.
            Otherwise, it is assumed the object is already in the store
            (default: None).
        key (str): specify the key to use to retrieve the object from the store.
            If `obj=None`, this argument is required. If `obj` is provided but
            the key is not, a key will be generated (default: None).

    Raise:
        ValueError if both `obj` and `key` are `None`.
    """

    def __init__(
        self, obj: Optional[object] = None, *, key: Optional[str] = None
    ) -> None:
        """Init LocalFactory"""
        if obj is None and key is None:
            raise ValueError('At least one of obj and key must be specified')
        if key is None:
            key = ps.utils.create_key(obj)
        if obj is not None:
            if ps.store is None:
                ps.init_local_backend()
            ps.store.set(key, obj)
        self.key = key

    def resolve(self) -> Any:
        """Return object associated with key"""
        return ps.store.get(self.key)

    def __getnewargs_ex__(self):
        """Helper method for pickling"""
        return (None,), {'key': self.key}


class RedisFactory(Factory):
    """Factory for RedisStore

    Adds support for asynchronously retrieving objects from a
    :class:`RedisStore <proxystore.backend.store.RedisStore>` backend and
    optional, strict guarentees on object versions.

    The :class:`RedisFactory <.RedisFactory>` also stores the hostname and
    port of the Redis server so a connection to the Redis server can be
    established if the proxy containing this factory is passed to a different
    process or machine.

    Args:
        obj (object): optionally pass the object that this factory will
            resolve. If passed, the object will be placed in the store.
            Otherwise, if `obj=None`, it is assumed the object is already in
            the store (default: None).
        key (str): key used to retrive object from Redis. If `None`, a key
            will be generated (default: None).
        hostname (str): hostname of Redis server. If `None`, the hostname
            from the current Redis backend will be used (default: None).
        port (int): port Redis server is listening on. If `None`, the port
            from the current Redis backend will be used (default: None).
        serialize (bool): if `True`, object in store is serialized and
            should be deserialized upon retrival (default: `True`).
        strict (bool): if `True`, ensures that the underlying object
            retrieved from the store is the most up to date version.
            Otherwise, an older version of an object associated with `key`
            may be returned if it is cached locally (default: `False`).

    Raises:
        ValueError if both `obj` and `key` are `None`.

    Raises:
        ValueError if one of `hostname` or `port` are `None` and the Redis
        backend has not been initialized yet.
    """

    def __init__(
        self,
        obj: Optional[object] = None,
        *,
        key: Optional[str] = None,
        hostname: Optional[str] = None,
        port: Optional[int] = None,
        serialize: bool = True,
        strict: bool = False,
    ) -> None:
        """Init RedisFactory"""
        if obj is None and key is None:
            raise ValueError('At least one of obj and key must be specified')
        if key is None:
            key = ps.utils.create_key(obj)
        if obj is not None:
            if ps.store is None:
                if hostname is None or port is None:
                    raise ValueError(
                        'One of hostname or port is None, but the backend '
                        'has not been initialized. Either initialize the '
                        'backend first or pass the Redis hostname and port'
                    )
                ps.init_redis_backend(hostname=hostname, port=port)
            ps.store.set(key, obj, serialize=serialize)
        if hostname is None:
            hostname = ps.store.hostname
        if port is None:
            port = ps.store.port

        self.key = key
        self.hostname = hostname
        self.port = port
        self.serialize = serialize
        self.strict = strict
        self.obj_future = None

    def __getnewargs_ex__(self):
        """Helper method for pickling"""
        return (None,), {
            'key': self.key,
            'hostname': self.hostname,
            'port': self.port,
            'serialize': self.serialize,
            'strict': self.strict,
        }

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
