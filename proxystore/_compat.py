"""Utilities for compatibility between ProxyStore versions.

Proxies, store configurations, and stream events are pickled and exchanged
between processes which may use different 2.x versions of ProxyStore.
Within a major version:

* The import paths of objects referenced by pickled data (e.g.,
  `proxystore.proxy._proxy_trampoline`, factory, config, and key types)
  must not change.
* Fields may be added to pickled state, connector configurations, and
  events if they have defaults. Fields must not be removed or renamed.
* Unknown fields, such as those added by a newer version, are ignored with
  a [`VersionMismatchWarning`][proxystore.warnings.VersionMismatchWarning].
"""

from __future__ import annotations

import inspect
import warnings
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from typing import Any

from proxystore.warnings import VersionMismatchWarning

STATE_VERSION_KEY = 'version'
"""Key of the format version in pickled state dictionaries."""


def drop_unknown_fields(
    kind: str,
    data: Mapping[str, Any],
    known: Iterable[str],
) -> dict[str, Any]:
    """Remove unknown fields from data and warn if any were found.

    Args:
        kind: Name of the type the data is for, used in the warning.
        data: Data which may contain unknown fields.
        known: Names of the known fields.

    Returns:
        `data` if all fields are known, otherwise a copy of `data` \
        containing only the known fields.
    """
    known = known if isinstance(known, (set, frozenset)) else set(known)
    if known.issuperset(data):
        # Fast path for the common case of the same version.
        return dict(data) if not isinstance(data, dict) else data
    unknown = sorted(set(data) - known)
    if len(unknown) > 0:
        warnings.warn(
            f'Ignoring unknown fields of {kind}: {", ".join(unknown)}. '
            'This can occur when objects are exchanged between processes '
            'using different versions of ProxyStore.',
            category=VersionMismatchWarning,
            stacklevel=3,
        )
    return {k: v for k, v in data.items() if k in known}


def init_kwargs(
    cls: Callable[..., Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    """Get the keyword arguments of `cls` from a configuration.

    Unknown keys are removed from the configuration with a warning.

    Args:
        cls: Class to get the parameters of the constructor of.
        config: Configuration of keyword arguments.

    Returns:
        Copy of `config` containing only the parameters of `cls`.
    """
    parameters = inspect.signature(cls).parameters
    if any(p.kind is p.VAR_KEYWORD for p in parameters.values()):
        return dict(config)
    name = getattr(cls, '__name__', repr(cls))
    return drop_unknown_fields(name, config, parameters)
