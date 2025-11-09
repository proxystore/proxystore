"""Common type definitions."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from typing import TypeVar

from proxystore.connectors.protocols import Connector
from proxystore.serialize import BytesLike

ConnectorT = TypeVar('ConnectorT', bound=Connector[Any])
"""Connector type variable."""
ConnectorKeyT = tuple[Any, ...]
"""Connector key type alias."""
SerializerT = Callable[[Any], BytesLike]
"""Serializer type alias."""
DeserializerT = Callable[[BytesLike], Any]
"""Deserializer type alias."""
