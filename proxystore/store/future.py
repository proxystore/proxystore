"""Proxy-future interface implementation."""
from __future__ import annotations

from typing import Generic
from typing import TypeVar

from proxystore.proxy import Proxy
from proxystore.store.factory import PollingStoreFactory
from proxystore.store.types import ConnectorT
from proxystore.store.types import SerializerT

T = TypeVar('T')


class ProxyFuture(Generic[T]):
    """Proxy-Future interface to a [`Store`][proxystore.store.base.Store].

    Args:
        factory: Factory that can resolve the object once it is resolved.
            This factory should block when resolving until the object is
            available.
        serializer: Use a custom serializer when setting the result object
            of this future.
    """

    def __init__(
        self,
        factory: PollingStoreFactory[ConnectorT, T],
        *,
        serializer: SerializerT | None = None,
    ) -> None:
        self._factory = factory
        self._serializer = serializer

    def proxy(self) -> Proxy[T]:
        """Create a proxy which will resolve to the result of this future."""
        return Proxy(self._factory)

    def result(self) -> T:
        """Get the result object of this future."""
        return self._factory.resolve()

    def set_result(self, obj: T) -> None:
        """Set the result object of this future.

        Args:
            obj: Result object.
        """
        self._factory.get_store()._set(
            self._factory.key,
            obj,
            serializer=self._serializer,
        )
