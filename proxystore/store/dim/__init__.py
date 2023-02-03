"""Distributed in-memory stores."""
from __future__ import annotations

import warnings

from proxystore.warnings import ExperimentalWarning

warnings.warn(
    'The proxystore.store.dim module contains experimental code that may '
    'change in the future or be unstable.',
    category=ExperimentalWarning,
    stacklevel=2,
)
