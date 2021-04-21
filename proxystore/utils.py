"""Utility functions for interacting with proxies"""
from typing import Any, Optional

import proxystore as ps
from proxystore.proxy import Proxy


def evict(proxy: Proxy) -> None:
    """Evicts value wrapped by proxy from backend store

    Useful for reducing memory used by the backend store when an object
    has been resolved for the last time.

    Note:
        if the proxy is not resolved, this function will force
        it to be resolved.

    Args:
        proxy (Proxy): `Proxy` instance to extract from

    Raise:
        RuntimeError:
            if the backend has not been initialized.
    """
    if ps.store is None:
        raise RuntimeError('Backend has not been initialized yet')
    if not is_resolved(proxy):
        resolve(proxy)
    key = get_key(proxy)
    if key is not None:
        ps.store.evict(key)


def extract(proxy: Proxy) -> Any:
    """Returns object wrapped by proxy

    If the proxy has not been resolved yet, this will force
    the proxy to be resolved prior.

    Args:
        proxy (Proxy): `Proxy` instance to extract from

    Returns:
        Wrapped object
    """
    return proxy.__wrapped__


def get_key(proxy: Proxy) -> Optional[str]:
    """Returns key associated object wrapped by proxy

    Keys are stored in the `Factory` passed to the `Proxy` constructor;
    however, not all `Factory` classes use a key (e.g., BaseFactory).

    Args:
        proxy (Proxy): `Proxy` instance to get key from

    Returns:
        (`str`) key if it exists otherwise `None`
    """
    if hasattr(proxy.__factory__, 'key'):
        return proxy.__factory__.key
    return None


def is_resolved(proxy: Proxy) -> bool:
    """Check if a proxy is resolved

    Args:
        proxy (Proxy): `Proxy` instance to check

    Returns:
        `True` if Proxy is resolved (i.e., the `Factory` has been called) and
        `False` otherwise
    """
    return proxy.__resolved__


def resolve(proxy: Proxy) -> None:
    """Force a proxy to resolve itself

    Args:
        proxy (Proxy): `Proxy` instance to check
    """
    proxy.__wrapped__


def resolve_async(proxy: Proxy) -> None:
    """Begin resolving proxy asynchronously

    Useful if the user knows a `Proxy` will be needed soon and wants to
    resolve the proxy concurrently with other computation.

    >>> ps.utils.resolve_async(my_proxy)
    >>> computation_without_proxy(...)
    >>> # p is hopefully resolved
    >>> computation_with_proxy(my_proxy, ...)

    Note:
        The asynchronous resolving functionality is implemented
        in `Factory.resolve_async()`.
        Most `Factory` implementations will store a future inside
        the `Factory` to the result and wait on that future the next
        time the `Proxy` is used.

    Args:
        proxy (Proxy): `Proxy` instance to check
    """
    if not is_resolved(proxy):
        proxy.__factory__.resolve_async()
