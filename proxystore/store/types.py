"""Common type definitions."""
from __future__ import annotations

from typing import Any
from typing import Callable
from typing import Tuple
from typing import TypeVar

from proxystore.connectors.protocols import Connector

ConnectorT = TypeVar('ConnectorT', bound=Connector[Any])
"""Connector type variable."""
ConnectorKeyT = Tuple[Any, ...]
"""Connector key type alias."""
SerializerT = Callable[[Any], bytes]
"""Serializer type alias."""
DeserializerT = Callable[[bytes], Any]
"""Deserializer type alias."""
