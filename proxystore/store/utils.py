"""Store utilities."""
from __future__ import annotations

from typing import NamedTuple
from typing import TypeVar

from proxystore.proxy import Proxy
from proxystore.store import base
from proxystore.store.exceptions import ProxyStoreFactoryError

T = TypeVar('T')


def get_key(proxy: Proxy[T]) -> NamedTuple:
    """Extract the key from the proxy's factory.

    Args:
        proxy (Proxy): proxy instance to get key from.

    Returns:
        The key, a NamedTuple unique to the
        :class:`~proxystore.store.base.Store` that created the proxy..

    Raises:
        ProxyStoreFactoryError:
            if the proxy's factory is not an instance of
            :class:`~proxystore.store.base.StoreFactory`.
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
