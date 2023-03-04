"""Endpoints for direct, cross-site communication.

Note:
   Please refer to the [Endpoints Guide](../../guides/endpoints.md) for an
   introduction to endpoints in ProxyStore.

[`Endpoints`][proxystore.endpoint.endpoint.Endpoint] are in-memory object
stores with peering capabilities. Endpoints enable peer-to-peer data transfer
between clients behind different NATs. See the
[`proxystore-endpoint`](../cli.md#proxystore-endpoint) CLI reference
to start your own endpoints.
"""
from __future__ import annotations

import warnings

from proxystore.warnings import ExperimentalWarning

warnings.warn(
    'Endpoints are an experimental feature and may change in the future.',
    category=ExperimentalWarning,
    stacklevel=2,
)
