"""Utilities for testing peer managers."""

from __future__ import annotations

import os
from typing import Any

import iroh

from proxystore.endpoint.config import EndpointP2PConfig
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.p2p.manager import PeerManager
from testing.utils import open_port


def local_peer_manager(
    proxystore_dir: str,
    name: str = 'peer',
    **kwargs: Any,
) -> PeerManager:
    """Create an endpoint and a peer manager which only uses localhost.

    The manager does not use relays or discovery so peers must be added with
    [`connect_peers()`][testing.p2p.connect_peers].

    Args:
        proxystore_dir: ProxyStore home directory to create the endpoint in.
        name: Name of the endpoint.
        kwargs: Options which override the defaults of the manager.
    """
    endpoint_dir = EndpointDir.create(
        name,
        proxystore_dir,
        port=open_port(),
        p2p=EndpointP2PConfig(relays='none'),
    )
    options: dict[str, Any] = {
        'preset': iroh.preset_minimal(),
        'bind_addr': '127.0.0.1:0',
        'addr_cache': None,
        **kwargs,
    }
    return PeerManager.from_endpoint_dir(endpoint_dir, **options)


def allow_peer(manager: PeerManager, peer: PeerManager, name: str) -> None:
    """Add a peer to the allowlist of a manager."""
    endpoint_dir = EndpointDir(os.path.dirname(manager._allowlist.path))
    endpoint_dir.peers.add(name, peer.id)


def connect_peers(*managers: PeerManager) -> None:
    """Allow started managers to communicate with each other."""
    for i, manager in enumerate(managers):
        for j, peer in enumerate(managers):
            if i != j:
                allow_peer(manager, peer, f'peer-{peer.id.short()}')
                manager.add_peer_addr(peer.addr())
