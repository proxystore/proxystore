"""Endpoints for direct, cross-site communication.

Note:
   Please refer to the [Endpoints Guide](../../guides/endpoints.md) for an
   introduction to endpoints in ProxyStore.

[`Endpoints`][proxystore.endpoint.endpoint.Endpoint] are in-memory object
stores with peering capabilities. Endpoints enable peer-to-peer data transfer
between clients behind different NATs. See the
[`proxystore-endpoint`](../cli.md#proxystore-endpoint) CLI reference
to start your own endpoints.

The public interface of endpoints is provided by these modules:

* [`client`][proxystore.endpoint.client]: Connect to a running endpoint.
* [`directory`][proxystore.endpoint.directory]: Create, find, and manage
  endpoints and the files in their directories.
* [`config`][proxystore.endpoint.config]: Endpoint configuration.
* [`identity`][proxystore.endpoint.identity]: Endpoint IDs and secret keys.
* [`peers`][proxystore.endpoint.peers]: Peers an endpoint communicates with.
* [`endpoint`][proxystore.endpoint.endpoint]: The endpoint object store.
* [`storage`][proxystore.endpoint.storage]: Storage used by an endpoint,
  which can be implemented to store data elsewhere.
* [`serve`][proxystore.endpoint.serve]: Run an endpoint in the current
  process.
* [`process`][proxystore.endpoint.process]: Start and stop endpoint
  processes.
* [`exceptions`][proxystore.endpoint.exceptions]: Endpoint errors.

The remaining modules ([`auth`][proxystore.endpoint.auth],
[`files`][proxystore.endpoint.files],
[`handler`][proxystore.endpoint.handler],
[`protocol`][proxystore.endpoint.protocol], and
[`server`][proxystore.endpoint.server]) and the
[`proxystore.endpoint.p2p`][proxystore.endpoint.p2p] package are internal
implementation details. They are documented for development, but their
interfaces may change between releases without notice.

Note:
    Endpoints and their clients (e.g., the
    [`EndpointConnector`][proxystore.connectors.endpoint.EndpointConnector])
    require the `endpoints` extra. Clients and endpoints share the ProxyStore
    home directory so they are expected to share a Python environment too.
"""

from __future__ import annotations

import importlib.util

_EXTRA_MODULES = ('aiosqlite', 'cryptography', 'daemon', 'iroh', 'uvloop')
_missing = [m for m in _EXTRA_MODULES if importlib.util.find_spec(m) is None]
if _missing:  # pragma: no cover
    raise ImportError(
        'Missing dependencies of ProxyStore Endpoints: '
        f'{", ".join(_missing)}. Install them with '
        '"pip install proxystore[endpoints]".',
    )
