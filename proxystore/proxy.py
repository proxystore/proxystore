"""ProxyStore Proxy Implementation"""
import random
from typing import Any, Optional

from lazy_object_proxy import slots

import proxystore as ps
import proxystore.backend.store as store
from proxystore.factory import BaseFactory


def _proxy_trampoline(factory: BaseFactory):
    """Trampoline for helping Proxy pickling

    `slots.Proxy` defines a property for ``__modules__`` which confuses
    pickle when trying to locate the class in the module. The trampoline is
    a top-level function so pickle can correctly find it in this module.

    Args:
        factory (BaseFactory): factory to pass to ``Proxy`` constructor.

    Returns:
        ``Proxy`` instance
    """
    return Proxy(factory)


class Proxy(slots.Proxy):
    """Lazy Object Proxy

    An extension of the Proxy from
    https://github.com/ionelmc/python-lazy-object-proxy with modified pickling
    behavior.

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

    Note:
        Due to ``Proxy`` modifying the ``__module__`` and ``__doc__``
        attributes, Sphinx cannot create autodocumentation for this
        class so any changes to the documentation here must be copied
        to ``docs/source/proxystore.proxy.rst``.

    Note:
        The `factory`, by default, is only ever called once during the
        lifetime of a proxy instance.

    Note:
        When a proxy instance is pickled, only the `factory` is pickled, not
        the wrapped object. Thus, proxy instances can be pickled and passed
        around cheaply, and once the proxy is unpickled and used, the `factory`
        will be called again to resolve the object.

    Args:
        factory (BaseFactory): callable object that returns the
            underlying object when called.

    Raises:
        TypeError:
            if `factory` is not an instance of `BaseFactory
            <proxystore.factory.BaseFactory>`.
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
        return _proxy_trampoline, (
            object.__getattribute__(self, '__factory__'),
        )

    def __reduce_ex__(self, protocol):
        """See `__reduce__`"""
        return self.__reduce__()


def to_proxy(
    obj: Any,
    key: Optional[str] = None,
    serialize: bool = True,
    strict: bool = False,
):
    """Place object in backend store and return :class:`.Proxy`

    This function automates the proxying process which involves:

    1. Creating a `key` for `obj` if one is not specified.
    2. Creating a `factory` for the object compatible with the current backend
       (e.g., :class:`RedisFactory <proxystore.factory.RedisFactory>` with a
       Redis backend).
    3. Creating a :class:`.Proxy` with the `factory`.
    4. Placing `obj` in the backend store.

    Args:
        obj: object to place in store and be proxied.
        key (str): specify key associated with `obj` in store. If `None`,
            a unique random key is generated (default: `None`).
        serialize (bool): serialized object before placing in
            backend. If `obj` has been manually serialized, set as `False`
            (default: `True`).
        strict (bool): if `True`, require store always returns most
            recent object associated with `key` (default: `False`).

    Returns:
        :class:`.Proxy` instance that will behave as and resolve `obj`.

    Raises:
        RuntimeError:
            if a backend has not been initialized, i.e.,
            :obj:`proxystore.store` is `None`. The backend should be initialized
            with one of the :func:`proxystore.init_{type}_backend()` functions.
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
        ps.store.set(key, obj)
    elif isinstance(ps.store, store.RedisStore):
        f = ps.factory.RedisFactory(
            key,
            hostname=ps.store.hostname,
            port=ps.store.port,
            serialize=serialize,
            strict=strict,
        )
        ps.store.set(key, obj, serialize=serialize)
    elif isinstance(ps.store, (store.BaseStore, store.CachedStore)):
        raise TypeError(
            'Backend of type {} is an abstract '
            'class!'.format(type(ps.store))
        )
    else:
        raise TypeError('Unrecognized backend type: {}'.format(type(ps.store)))

    return Proxy(f)
