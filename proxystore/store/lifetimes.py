"""Lifetime managers for objects in shared stores."""

from __future__ import annotations

import sys
from types import TracebackType
from typing import Any
from typing import Protocol
from typing import runtime_checkable
from typing import TYPE_CHECKING

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from proxystore.proxy import Proxy
from proxystore.store.exceptions import ProxyStoreFactoryError
from proxystore.store.factory import StoreFactory
from proxystore.store.types import ConnectorKeyT

if TYPE_CHECKING:
    from proxystore.store.base import Store


@runtime_checkable
class Lifetime(Protocol):
    """Lifetime protocol.

    Attributes:
        store: [`Store`][proxystore.store.base.Store] instance use to create
            the objects associated with this lifetime and that will be used
            to evict them when the lifetime has ended.
    """

    store: Store[Any]

    def close(self) -> None:
        """End the lifetime and evict all associated objects."""
        ...

    def done(self) -> bool:
        """Check if lifetime has ended."""
        ...

    def add_key(self, *keys: ConnectorKeyT) -> None:
        """Associate a new object with the lifetime.

        Warning:
            All keys should have been created by the same
            [`Store`][proxystore.store.base.Store] that this lifetime was
            initialized with.

        Args:
            keys: One or more keys of objects to associate with this lifetime.
        """
        ...

    def add_proxy(self, *proxies: Proxy[Any]) -> None:
        """Associate a new object with the lifetime.

        Warning:
            All proxies should have been created by the same
            [`Store`][proxystore.store.base.Store] that this lifetime was
            initialized with.

        Args:
            proxies: One or more proxies of objects to associate with this
                lifetime.

        Raises:
            ProxyStoreFactoryError: If the proxy's factory is not an instance
                of [`StoreFactory`][proxystore.store.base.StoreFactory].
        """
        ...


class ContextLifetime:
    """Basic lifetime manager.

    Basic object lifetime manager with context manager support.

    Args:
        store: [`Store`][proxystore.store.base.Store] instance use to create
            the objects associated with this lifetime and that will be used
            to evict them when the lifetime has ended.
    """

    def __init__(self, store: Store[Any]) -> None:
        self.store = store
        self._done = False
        self._keys: set[ConnectorKeyT] = set()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """End the lifetime and evict all associated objects."""
        for key in self._keys:
            self.store.evict(key)
        self._done = True

    def done(self) -> bool:
        """Check if lifetime has ended."""
        return self._done

    def add_key(self, *keys: ConnectorKeyT) -> None:
        """Associate a new object with the lifetime.

        Warning:
            All keys should have been created by the same
            [`Store`][proxystore.store.base.Store] that this lifetime was
            initialized with.

        Args:
            keys: One or more keys of objects to associate with this lifetime.
        """
        self._keys.update(keys)

    def add_proxy(self, *proxies: Proxy[Any]) -> None:
        """Associate a new object with the lifetime.

        Warning:
            All proxies should have been created by the same
            [`Store`][proxystore.store.base.Store] that this lifetime was
            initialized with.

        Args:
            proxies: One or more proxies of objects to associate with this
                lifetime.

        Raises:
            ProxyStoreFactoryError: If the proxy's factory is not an instance
                of [`StoreFactory`][proxystore.store.base.StoreFactory].
        """
        keys: list[ConnectorKeyT] = []
        for proxy in proxies:
            factory = proxy.__factory__
            if isinstance(factory, StoreFactory):
                keys.append(factory.key)
            else:
                raise ProxyStoreFactoryError(
                    'The proxy must contain a factory with type '
                    f'{type(StoreFactory).__name__}. {type(factory).__name__} '
                    'is not supported.',
                )
        self.add_key(*keys)
