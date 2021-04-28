"""Utility functions for interacting with proxies"""
from __future__ import annotations

from typing import Any, Optional

import proxystore as ps


def evict(proxy: 'ps.proxy.Proxy') -> None:
    """Evicts value wrapped by proxy from backend store

    Useful for reducing memory used by the backend store when an object
    has been resolved for the last time.

    Note:
        If `proxy` is not resolved, this function will force
        it to be resolved.

    Args:
        proxy (Proxy): proxy wrapping object to be evicted.

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


def extract(proxy: 'ps.proxy.Proxy') -> Any:
    """Returns object wrapped by proxy

    If the proxy has not been resolved yet, this will force
    the proxy to be resolved prior.

    Args:
        proxy (Proxy): proxy instance to extract from.

    Returns:
        object wrapped by proxy.
    """
    return proxy.__wrapped__


def get_key(proxy: 'ps.proxy.Proxy') -> Optional[str]:
    """Returns key associated object wrapped by proxy

    Keys are stored in the `factory` passed to the
    :class:`Proxy <proxystore.proxy.Proxy>` constructor; however, not all
    :mod:`Factory <proxystore.factory>` classes use a key
    (e.g., :class:`BaseFactory <proxystore.factory.BaseFactory>`).

    Args:
        proxy (Proxy): proxy instance to get key from.

    Returns:
        key (`str`) if it exists otherwise `None`.
    """
    if hasattr(proxy.__factory__, 'key'):
        return proxy.__factory__.key
    return None


def is_resolved(proxy: 'ps.proxy.Proxy') -> bool:
    """Check if a proxy is resolved

    Args:
        proxy (Proxy): proxy instance to check.

    Returns:
        `True` if `proxy` is resolved (i.e., the `factory` has been called) and
        `False` otherwise.
    """
    return proxy.__resolved__


def resolve(proxy: 'ps.proxy.Proxy') -> None:
    """Force a proxy to resolve itself

    Args:
        proxy (Proxy): proxy instance to force resolve.
    """
    proxy.__wrapped__


def resolve_async(proxy: 'ps.proxy.Proxy') -> None:
    """Begin resolving proxy asynchronously

    Useful if the user knows a proxy will be needed soon and wants to
    resolve the proxy concurrently with other computation.

    >>> ps.utils.resolve_async(my_proxy)
    >>> computation_without_proxy(...)
    >>> # p is hopefully resolved
    >>> computation_with_proxy(my_proxy, ...)

    Note:
        The asynchronous resolving functionality is implemented
        in :func:`BaseFactory.resolve_async()
        <proxystore.factory.BaseFactory.resolve_async()>`.
        Most :mod:`Factory <proxystore.factory>` implementations will store a
        future to the result and wait on that future the next
        time the proxy is used.

    Args:
        proxy (Proxy): proxy instance to begin asynchronously resolving.
    """
    if not is_resolved(proxy):
        proxy.__factory__.resolve_async()
