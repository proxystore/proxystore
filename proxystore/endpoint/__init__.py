"""Endpoints for direct, cross-site communication.

Note:
   Please refer to the [Endpoints Guide](../../guides/endpoints.md) for an
   introduction to endpoints in ProxyStore.

[`Endpoints`][proxystore.endpoint.endpoint.Endpoint] are in-memory object
stores with peering capabilities. Endpoints enable peer-to-peer data transfer
between clients behind different NATs. See the
[`proxystore-endpoint`](../cli.md#proxystore-endpoint) CLI reference
to start your own endpoints.

The public interface of endpoints is exported by this package (see
`proxystore.endpoint.__all__`) and is organized by role:

* Files shared by endpoints and clients:
  [`directory`][proxystore.endpoint.directory] (creating, finding, and
  managing endpoints and the files in their directories),
  [`config`][proxystore.endpoint.config] (configuration),
  [`identity`][proxystore.endpoint.identity] (endpoint IDs and secret keys),
  and [`peers`][proxystore.endpoint.peers] (the peers an endpoint
  communicates with).
* Clients: [`client`][proxystore.endpoint.client] (connect to a running
  endpoint).
* Running endpoints: [`endpoint`][proxystore.endpoint.endpoint] (run an
  endpoint), [`storage`][proxystore.endpoint.storage] (storage of objects
  which can be implemented to store objects elsewhere), and
  [`process`][proxystore.endpoint.process] (run, start, and stop endpoint
  processes).
* Errors: [`exceptions`][proxystore.endpoint.exceptions] and
  [`warnings`][proxystore.endpoint.warnings].

The remaining modules implement the endpoint and are documented for
development: [`auth`][proxystore.endpoint.auth] (client authentication),
[`dispatch`][proxystore.endpoint.dispatch] (handling requests),
[`files`][proxystore.endpoint.files] (reading and writing files),
[`protocol`][proxystore.endpoint.protocol] (the wire protocol),
[`server`][proxystore.endpoint.server] (the server which accepts client
connections), and the [`p2p`][proxystore.endpoint.p2p] package
(communication with peers). Names which are not exported by this package may
change between releases without notice.

Note:
    Endpoints and their clients (e.g., the
    [`EndpointConnector`][proxystore.connectors.endpoint.EndpointConnector])
    require the `endpoints` extra. Clients and endpoints share the ProxyStore
    home directory so they are expected to share a Python environment too.
"""

# ruff: noqa: E402
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

# The imports are after the check for the dependencies of endpoints, so
# E402 (module level import not at top of file) is ignored in this file.
from proxystore.endpoint.client import EndpointClient
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import EndpointP2PConfig
from proxystore.endpoint.config import EndpointStorageConfig
from proxystore.endpoint.directory import ConnectionInfo
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.directory import EndpointStatus
from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.exceptions import EndpointAuthError
from proxystore.endpoint.exceptions import EndpointConfigError
from proxystore.endpoint.exceptions import EndpointConnectionError
from proxystore.endpoint.exceptions import EndpointConnectorError
from proxystore.endpoint.exceptions import EndpointError
from proxystore.endpoint.exceptions import EndpointExistsError
from proxystore.endpoint.exceptions import EndpointNotFoundError
from proxystore.endpoint.exceptions import EndpointNotRunningError
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.exceptions import EndpointRequestError
from proxystore.endpoint.exceptions import EndpointRunningError
from proxystore.endpoint.exceptions import ObjectSizeExceededError
from proxystore.endpoint.exceptions import PeerConnectionTimeoutError
from proxystore.endpoint.exceptions import PeerError
from proxystore.endpoint.exceptions import PeerExistsError
from proxystore.endpoint.exceptions import PeeringDisabledError
from proxystore.endpoint.exceptions import PeerNotAllowedError
from proxystore.endpoint.exceptions import PeerNotFoundError
from proxystore.endpoint.exceptions import PeerUnavailableError
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.identity import SecretKey
from proxystore.endpoint.p2p.manager import PeerOptions
from proxystore.endpoint.p2p.manager import PeerPolicy
from proxystore.endpoint.peers import Peers
from proxystore.endpoint.peers import PeersConfig
from proxystore.endpoint.process import serve
from proxystore.endpoint.process import start_endpoint
from proxystore.endpoint.process import stop_endpoint
from proxystore.endpoint.protocol import EndpointInfo
from proxystore.endpoint.protocol import PingResult
from proxystore.endpoint.storage import MemoryStorage
from proxystore.endpoint.storage import SQLiteStorage
from proxystore.endpoint.storage import Storage
from proxystore.endpoint.warnings import EndpointVersionWarning

__all__ = [
    'ConnectionInfo',
    'Endpoint',
    'EndpointAuthError',
    'EndpointClient',
    'EndpointConfig',
    'EndpointConfigError',
    'EndpointConnectionError',
    'EndpointConnectorError',
    'EndpointDir',
    'EndpointError',
    'EndpointExistsError',
    'EndpointId',
    'EndpointInfo',
    'EndpointNotFoundError',
    'EndpointNotRunningError',
    'EndpointP2PConfig',
    'EndpointProtocolError',
    'EndpointRequestError',
    'EndpointRunningError',
    'EndpointStatus',
    'EndpointStorageConfig',
    'EndpointVersionWarning',
    'MemoryStorage',
    'ObjectSizeExceededError',
    'PeerConnectionTimeoutError',
    'PeerError',
    'PeerExistsError',
    'PeerNotAllowedError',
    'PeerNotFoundError',
    'PeerOptions',
    'PeerPolicy',
    'PeerUnavailableError',
    'PeeringDisabledError',
    'Peers',
    'PeersConfig',
    'PingResult',
    'SQLiteStorage',
    'SecretKey',
    'Storage',
    'serve',
    'start_endpoint',
    'stop_endpoint',
]
