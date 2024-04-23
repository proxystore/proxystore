"""Common type definitions."""

from __future__ import annotations

import sys
from typing import Any
from typing import Callable
from typing import Tuple
from typing import TypedDict
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import NotRequired
else:  # pragma: <3.11 cover
    from typing_extensions import NotRequired

from proxystore.connectors.protocols import Connector

ConnectorT = TypeVar('ConnectorT', bound=Connector[Any])
"""Connector type variable."""
ConnectorKeyT = Tuple[Any, ...]
"""Connector key type alias."""
SerializerT = Callable[[Any], bytes]
"""Serializer type alias."""
DeserializerT = Callable[[bytes], Any]
"""Deserializer type alias."""


class StoreConfig(TypedDict):
    """Store configuration dictionary.

    Warning:
        Configuration dictionaries should not be constructed manually, but
        instead created via
        [`Store.config()`][proxystore.store.base.Store.config].

    Tip:
        See the [`Store`][proxystore.store.base.Store] parameters for more
        information about each configuration option.

    Attributes:
        name: Store name.
        connector_type: Fully qualified path to the
            [`Connector`][proxystore.connectors.protocols.Connector]
            implementation.
        connector_config: Config created by
            [`Connector.config()`][proxystore.connectors.protocols.Connector.config]
            used to initialize a new connector instance with
            [`Connector.from_config()`][proxystore.connectors.protocols.Connector.from_config].
        serializer: Optional serializer.
        deserializer: Optional deserializer.
        cache_size: Cache size.
        metrics: Enable recording operation metrics.
        register: Auto-register the store.
    """

    name: str
    connector_type: str
    connector_config: dict[str, Any]
    serializer: NotRequired[SerializerT | None]
    deserializer: NotRequired[DeserializerT | None]
    cache_size: NotRequired[int]
    metrics: NotRequired[bool]
    register: NotRequired[bool]
