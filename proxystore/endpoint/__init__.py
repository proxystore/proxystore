"""Endpoints for direct, cross-site communication.

Note:
   Please refer to the [Endpoints Guide](../../guides/endpoints.md) for an
   introduction to endpoints in ProxyStore.

[`Endponts`][proxystore.endpoint.endpoint.Endpoint] are in-memory object
stores with peering capabilities. Endpoints enable data transfer between
multiple sites using NAT traversal.
"""
from __future__ import annotations

import warnings

from proxystore.warnings import ExperimentalWarning

warnings.warn(
    'Endpoints are an experimental feature and may change in the future.',
    category=ExperimentalWarning,
    stacklevel=2,
)
