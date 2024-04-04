"""Resolve imports by string paths."""

from __future__ import annotations

import importlib
from typing import Any


def get_object_path(obj: Any) -> str:
    """Get the fully qualified path of an object.

    Example:
        ```python
        >>> from proxystore.connectors.protocols import Connector
        >>> get_object_path(Connector)
        'proxystore.connectors.protocols.Connector'
        ```

    Args:
        obj: Object to get fully qualified path of.

    Returns:
        Fully qualified path of `obj`.
    """
    return f'{obj.__module__}.{obj.__qualname__}'


def import_from_path(path: str) -> type[Any]:
    """Import object via its fully qualified path.

    Example:
        ```python
        >>> import_from_path('proxystore.connectors.protocols.Connector')
        <class 'proxystore.connectors.protocols.Connector'>
        ```

    Args:
        path: Fully qualified path of object to import.

    Returns:
        Imported object.

    Raises:
        ImportError: If an object at the `path` is not found.
    """
    module_path, _, name = path.rpartition('.')
    if len(module_path) == 0:
        raise ImportError(
            f'Object path must contain at least one module. Got {path}',
        )
    module = importlib.import_module(module_path)
    return getattr(module, name)
