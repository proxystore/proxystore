"""Store utilities."""
from __future__ import annotations

from typing import Any
from typing import Tuple
from typing import TypeVar

from proxystore.proxy import is_resolved
from proxystore.proxy import Proxy
from proxystore.store import base
from proxystore.store.exceptions import ProxyStoreFactoryError

T = TypeVar('T')
ConnectorKeyT = Tuple[Any, ...]
"""Connector key type alias."""


def get_key(proxy: Proxy[T]) -> ConnectorKeyT:
    """Extract the key from the proxy's factory.

    Args:
        proxy: Proxy instance to get key from.

    Returns:
        The key, a NamedTuple unique to the \
        [`Store`][proxystore.store.base.Store] that created the proxy..

    Raises:
        ProxyStoreFactoryError: If the proxy's factory is not an instance of
            [`StoreFactory`][proxystore.store.base.StoreFactory].
    """
    factory = proxy.__factory__
    if isinstance(factory, base.StoreFactory):
        return factory.key
    else:
        raise ProxyStoreFactoryError(
            'The proxy must contain a factory with type '
            f'{type(base.StoreFactory).__name__}. {type(factory).__name__} '
            'is not supported.',
        )


def resolve_async(proxy: Proxy[T]) -> None:
    """Begin resolving proxy asynchronously.

    Useful if the user knows a proxy will be needed soon and wants to
    resolve the proxy concurrently with other computation.

    ```python
    from proxystore.store.utils import resolve_async

    resolve_async(my_proxy)
    computation_without_proxy(...)
    # p is hopefully resolved
    computation_with_proxy(my_proxy, ...)
    ```

    Note:
        The asynchronous resolving functionality is implemented
        by [`StoreFactory`][proxystore.store.base.StoreFactory]. Factories that
        are not of this type will error when used with this function.

    Args:
        proxy: Proxy instance to begin asynchronously resolving.
    """
    if not is_resolved(proxy):
        proxy.__factory__.resolve_async()
