"""Store utilities."""

from __future__ import annotations

from typing import TypeVar

from proxystore.proxy import get_factory
from proxystore.proxy import is_resolved
from proxystore.proxy import Proxy
from proxystore.store import base
from proxystore.store.exceptions import ProxyStoreFactoryError
from proxystore.store.types import ConnectorKeyT

T = TypeVar('T')


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
    factory = get_factory(proxy)
    if isinstance(factory, base.StoreFactory):
        return factory.key
    else:
        raise ProxyStoreFactoryError(
            'The proxy must contain a factory with type '
            f'{base.StoreFactory.__name__}. {type(factory).__name__} '
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

    Raises:
        ProxyStoreFactoryError: If the proxy's factory is not an instance of
            [`StoreFactory`][proxystore.store.base.StoreFactory].
    """
    factory = get_factory(proxy)
    if isinstance(factory, base.StoreFactory):
        if not is_resolved(proxy):
            factory.resolve_async()
    else:
        raise ProxyStoreFactoryError(
            'The proxy must contain a factory with type '
            f'{base.StoreFactory.__name__}. {type(factory).__name__} '
            'is not supported.',
        )
