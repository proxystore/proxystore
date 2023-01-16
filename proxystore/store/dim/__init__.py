"""Distributed in-memory stores."""
from __future__ import annotations

import warnings


class ExperimentalWarning(Warning):
    """Warning for experimental features."""

    pass


warnings.warn(
    'The proxystore.store.dim module contains experimental code that may have '
    'known issues or is subject to change in future releases. See the issue '
    'tracker for the latest updates: '
    'https://github.com/proxystore/proxystore/issues.',
    ExperimentalWarning,
    stacklevel=2,
)
