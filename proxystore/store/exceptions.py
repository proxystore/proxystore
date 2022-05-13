"""Exceptions for Stores."""
from __future__ import annotations

from proxystore import store


class ProxyResolveMissingKey(Exception):
    """Exception raised when the key associated with a proxy is missing."""

    def __init__(
        self,
        key: str,
        store_type: type[store.base.Store],
        store_name: str,
    ) -> None:
        """Init ProxyResolveMissingKey.

        Args:
            key (str): key associated with target object that could not be
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
