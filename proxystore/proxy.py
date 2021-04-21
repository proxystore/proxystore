"""ProxyStore Proxy Implementation"""
import lazy_object_proxy
import random
from typing import Any, Optional

import proxystore as ps
import proxystore.backend.store as store
from proxystore.factory import BaseFactory


class Proxy(lazy_object_proxy.Proxy):
    """Lazy Object Proxy

    An extension of the `Proxy` from
    https://github.com/ionelmc/python-lazy-object-proxy with better pickling
    support.

    An object proxy acts as a thin wrapper around a Python object, i.e.
    the proxy behaves identically to the underlying object. The proxy is
    initialized with a callable factory object. The factory returns the
    underlying object when called, i.e. 'resolves' the proxy. The proxy
    does not call the factory until the first access to the proxy (hence, the
    lazy aspect of the proxy).

    The factory contains the mechanisms to appropriately resolve the object,
    e.g., which in the case for ProxyStore means requesting the correct
    object from the backend store.

    >>> x = np.array([1, 2, 3])
    >>> f = ps.factory.BaseFactory(x)
    >>> p = ps.proxy.Proxy(f)
    >>> assert isinstance(p, np.ndarray)
    >>> assert np.array_equal(p, [1, 2, 3])

    Args:
        factory (BaseFactory): callable object that returns the
            underlying object when called

    Raises:
        TypeError:
            if `factory` is not an instance of `BaseFactory`
    """

    def __init__(self, factory: BaseFactory) -> None:
        """Init Proxy"""
        if not isinstance(factory, BaseFactory):
            raise TypeError('factory must be of type ps.factory.BaseFactory')
        super(Proxy, self).__init__(factory)

    def __reduce__(self):
        """Helper method for pickling

        Override `Proxy.__reduce__` so that we only pickle the Factory
        and not the object itself to reduce size of the pickle.
        """
        return Proxy, (self.__factory__,)

    def __reduce_ex__(self, protocol):
        """See `__reduce__`"""
        return self.__reduce__()


def to_proxy(obj: Any, key: Optional[str] = None, strict: bool = False):
    """Place object in backend store and return Proxy reference

    This function automates the proxying process which involves:
    1) creating a key for the object if one is not specified,
    2) creating a `Factory` for the object compatible with the current backend
    (e.g., `RedisFactory` with a Redis backend)
    3) creating a `Proxy` with the `Factory`, and
    4) putting the object in the store with the key.

    Args:
        obj: object to place in store and be proxied.
        key (str, optional): specify key associated with `obj` in store. If
            `key` is unspecified, a unique random key is generated.
        strict (bool): if `True`, require store always returns most recent
            object associated with `key` (default: False).

    Returns:
        `Proxy` that acts as `obj`.

    Raises:
        RuntimeError:
            if a backend has not been initialized, i.e.,
            `proxystore.store` is `None`. The backend should be initialized
            with one of the `proxystore.init_{type}_backend()` functions.
    """
    if ps.store is None:
        raise RuntimeError('Backend store is not initialized yet')

    if key is None:
        # Make sure we don't have a key collision
        # TODO(gpauloski): consider key based on object hash so
        # identical objects are not duplicated?
        key = str(random.getrandbits(128))
        while ps.store.exists(key):
            key = str(random.getrandbits(128))  # pragma: no cover

    if isinstance(ps.store, store.LocalStore):
        f = ps.factory.KeyFactory(key)
    elif isinstance(ps.store, store.RedisStore):
        f = ps.factory.RedisFactory(
            key, hostname=ps.store.hostname, port=ps.store.port, strict=strict
        )
    elif isinstance(ps.store, (store.BaseStore, store.CachedStore)):
        raise TypeError(
            'Backend of type {} is an abstract '
            'class!'.format(type(ps.store))
        )
    else:
        raise TypeError('Unrecognized backend type: {}'.format(type(ps.store)))

    ps.store.set(key, obj)
    return Proxy(f)
