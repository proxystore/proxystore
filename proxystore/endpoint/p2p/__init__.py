"""Peer-to-peer communication between endpoints.

Endpoints communicate with peer endpoints over
[iroh](https://www.iroh.computer/){target=_blank}, a QUIC-based peer-to-peer
library. Endpoints are addressed by their
[`EndpointId`][proxystore.endpoint.identity.EndpointId] (the public key of
the endpoint), and iroh handles NAT traversal, falling back to relaying
traffic when a direct connection cannot be established, and discovery of the
addresses of peers. Connections are encrypted and authenticated with TLS 1.3
so each endpoint knows the ID of its peer.

The [`PeerManager`][proxystore.endpoint.p2p.manager.PeerManager] only
communicates with the peers allowed by its
[`PeerPolicy`][proxystore.endpoint.p2p.manager.PeerPolicy], which is the
endpoint's allowlist (see
[`proxystore.endpoint.peers`][proxystore.endpoint.peers]).
"""

from __future__ import annotations
