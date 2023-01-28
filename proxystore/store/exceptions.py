"""Exceptions for Stores."""
from __future__ import annotations

from typing import NamedTuple

from proxystore.store import base


class StoreError(Exception):
    """Base exception class for store errors."""

    pass


class StoreExistsError(StoreError):
    """Exception raised when a store with the same name already exists."""

    pass


class ProxyStoreFactoryError(StoreError):
    """Exception raised when a proxy was not created by a Store."""

    pass


class ProxyResolveMissingKeyError(Exception):
    """Exception raised when the key associated with a proxy is missing."""

    def __init__(
        self,
        key: NamedTuple,
        store_type: type[base.Store[base.KeyT]],
        store_name: str,
    ) -> None:
        """Init ProxyResolveMissingKeyError.

        Args:
            key (tuple): key associated with target object that could not be
                found in the store.
            store_type (Store): type of store that the key could not be found
                in.
            store_name (str): name of store that the key could not be found in.
        """
        self.key = key
        self.store_type = store_type
        self.store_name = store_name
        super().__init__(
            f"Proxy cannot resolve target object with key='{self.key}' "
            f"from {self.store_type.__name__}(name='{self.store_name}'): "
            'store returned NoneType with key.',
        )
