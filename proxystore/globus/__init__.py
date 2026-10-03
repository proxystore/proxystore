"""ProxyStore Globus SDK tools.

Warning:
    This module is an internal implementation detail which may change
    between releases without notice (see
    [Versioning and Compatibility](../../versioning.md)).

The Globus Auth flows are largely based on
[Globus Compute's implementation](https://github.com/funcx-faas/funcX/tree/2.3.2/compute_sdk/globus_compute_sdk/sdk/login_manager).
"""

from __future__ import annotations

import importlib.util

_EXTRA_MODULES = ('click', 'globus_sdk')
_missing = [m for m in _EXTRA_MODULES if importlib.util.find_spec(m) is None]
if _missing:  # pragma: no cover
    raise ImportError(
        'Missing dependencies of the ProxyStore Globus tools: '
        f'{", ".join(_missing)}. Install them with '
        '"pip install proxystore[globus]".',
    )
