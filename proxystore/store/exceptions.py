"""Exceptions for Stores."""

from __future__ import annotations

from typing import Any
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from proxystore.store import base


def __getattr__(name: str) -> type[StoreError]:
    # StoreExistsError was removed in v2. It is loaded lazily from the
    # compatibility module so that it is not documented.
    if name == 'StoreExistsError':
        from proxystore.store._compat import StoreExistsError

        return StoreExistsError
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')


class StoreError(Exception):
    """Base exception class for store errors."""


class ProxyStoreFactoryError(StoreError):
    """Exception raised when a proxy was not created by a Store."""


class ProxyResolveMissingKeyError(StoreError):
    """Exception raised when the key associated with a proxy is missing."""

    def __init__(
        self,
        key: base.ConnectorKeyT,
        store_type: type[base.Store[Any]],
        store_name: str | None,
        store_id: str | None = None,
    ) -> None:
        """Init ProxyResolveMissingKeyError.

        Args:
            key: Key associated with target object that could not be found in
                the store.
            store_type: Type of store that the key could not be found in.
            store_name: Name of store that the key could not be found in.
            store_id: ID of store that the key could not be found in.
        """
        self.key = key
        self.store_type = store_type
        self.store_name = store_name
        self.store_id = store_id
        store = ', '.join(
            f'{attr}={value!r}'
            for attr, value in (('id', store_id), ('name', store_name))
            if value is not None
        )
        super().__init__(
            f"Cannot resolve target object with key='{self.key}' "
            f'from {self.store_type.__name__}({store}) '
            'because there is no object associated with the key. This can '
            'often occur when the target object is evicted from the store '
            'while proxies of the target still exist.',
        )


class NonProxiableTypeError(StoreError):
    """Exception raised when proxying an unproxiable type."""
