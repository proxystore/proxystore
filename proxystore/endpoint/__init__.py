"""Endpoints for direct, cross-site communication.

Note:
   Please refer to the [Endpoints Guide](../../guides/endpoints.md) for an
   introduction to endpoints in ProxyStore.

[`Endpoints`][proxystore.endpoint.endpoint.Endpoint] are in-memory object
stores with peering capabilities. Endpoints enable peer-to-peer data transfer
between clients behind different NATs. See the
[`proxystore-endpoint`](../cli.md#proxystore-endpoint) CLI reference
to start your own endpoints.

The public Python interface of endpoints is the following modules:

* [`client`][proxystore.endpoint.client]: connect to a running endpoint.
* [`exceptions`][proxystore.endpoint.exceptions]: errors raised by
  endpoints and their clients.
* [`warnings`][proxystore.endpoint.warnings]: warnings raised by clients.

Endpoints are otherwise used through the
[`proxystore-endpoint`](../cli.md#proxystore-endpoint) CLI and the
[`EndpointConnector`][proxystore.connectors.endpoint.EndpointConnector],
and the formats of their files and protocols are versioned (see the
[Endpoints Guide](../../guides/endpoints.md#version-compatibility)).

The remaining modules are internal implementation details which may change
between releases without notice. They are documented for development:

* Files shared by endpoints and clients:
  [`directory`][proxystore.endpoint.directory] (creating, finding, and
  managing endpoints and the files in their directories),
  [`config`][proxystore.endpoint.config] (configuration),
  [`identity`][proxystore.endpoint.identity] (endpoint IDs and secret keys),
  [`peers`][proxystore.endpoint.peers] (the peers an endpoint communicates
  with), and [`files`][proxystore.endpoint.files] (reading and writing
  files).
* Running endpoints: [`endpoint`][proxystore.endpoint.endpoint] (run an
  endpoint), [`process`][proxystore.endpoint.process] (run, start, and stop
  endpoint processes), [`storage`][proxystore.endpoint.storage] (storage of
  objects), and [`cli`][proxystore.endpoint.cli] (the implementation of the
  CLI).
* Communication: [`protocol`][proxystore.endpoint.protocol] (the wire
  protocol), [`auth`][proxystore.endpoint.auth] (client authentication),
  [`server`][proxystore.endpoint.server] (the server which accepts client
  connections), [`dispatch`][proxystore.endpoint.dispatch] (handling
  requests), and the [`p2p`][proxystore.endpoint.p2p] package
  (communication with peers).

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
