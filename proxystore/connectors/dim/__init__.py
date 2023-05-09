"""Distributed in-memory store connectors."""
from __future__ import annotations

import warnings

from proxystore.warnings import ExperimentalWarning

warnings.warn(
    'The proxystore.connectors.dim module is experimental and may be '
    'moved to the proxystore-extensions package in the future.',
    category=ExperimentalWarning,
    stacklevel=2,
)
