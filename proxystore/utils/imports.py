"""Resolve imports by string paths."""
from __future__ import annotations

import importlib
from typing import Any


def get_class_path(cls: type[Any]) -> str:
    """Get the fully qualified path of a type.

    Example:
        ```python
        >>> from proxystore.connectors.protocols import Connector
        >>> get_class_path(Connector)
        'proxystore.connectors.protocols.Connector'
        ```

    Args:
        cls: Class type to get fully qualified path of.

    Returns:
        Fully qualified path of `cls`.
    """
    return f'{cls.__module__}.{cls.__qualname__}'


def import_class(path: str) -> type[Any]:
    """Import class via its fully qualified path.

    Example:
        ```python
        >>> import_class('proxystore.connectors.protocols.Connector')
        <class 'proxystore.connectors.protocols.Connector'>
        ```

    Args:
        path: Fully qualified path of class to import.

    Returns:
        Imported class.

    Raises:
        ImportError: If a class at the `path` is not found.
    """
    module_path, _, name = path.rpartition('.')
    if len(module_path) == 0:
        raise ImportError(
            f'Class path must contain at least one module. Got {path}',
        )
    module = importlib.import_module(module_path)
    return getattr(module, name)
