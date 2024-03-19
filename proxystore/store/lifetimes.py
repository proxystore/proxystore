"""Lifetime managers for objects in shared stores."""

from __future__ import annotations

import logging
import sys
import uuid
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

logger = logging.getLogger(__name__)


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

    Object lifetime manager with context manager support.

    Example:
        ```python
        from proxystore.store.base import Store
        from proxystore.store.lifetimes import ContextLifetime

        store = Store(...)

        with ContextLifetime(store) as lifetime:
            # Objects in the store can be associated with this lifetime.
            key = store.put('value', lifetime=lifetime)
            proxy = store.proxy('value', lifetime=lifetime)

        # Objects associated with the lifetime are evicted once the
        # lifetime ends.
        assert not store.exists(key)

        store.close()
        ```

    Args:
        store: [`Store`][proxystore.store.base.Store] instance use to create
            the objects associated with this lifetime and that will be used
            to evict them when the lifetime has ended.
        name: Specify a name for this lifetime used in logging. Otherwise,
            a unique ID will be generated.
    """

    def __init__(
        self,
        store: Store[Any],
        *,
        name: str | None = None,
    ) -> None:
        self.store = store
        self.name = name if name is not None else str(uuid.uuid4())
        self._done = False
        self._keys: set[ConnectorKeyT] = set()

        logger.info(f'Initialized lifetime manager (name={self.name})')

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
        logger.info(
            f'Closed lifetime manager and evicted {len(self._keys)} '
            f'associated objects (name={self.name})',
        )
        self._keys.clear()

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
        logger.debug(
            f'Added keys to lifetime manager (name={self.name}): '
            f'{", ".join(repr(key) for key in keys)}',
        )

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
