"""Utilities for testing peer managers."""

from __future__ import annotations

import os
from typing import Any

import iroh

from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.identity import SecretKey
from proxystore.endpoint.peers import Allowlist
from proxystore.endpoint.peers import PeersConfig
from proxystore.p2p.manager import PeerManager


def local_peer_manager(path: str, **kwargs: Any) -> PeerManager:
    """Create a peer manager which only communicates on localhost.

    The manager does not use relays or discovery so peers must be added with
    [`connect_peers()`][testing.p2p.connect_peers].

    Args:
        path: Directory for the allowlist of the manager.
        kwargs: Extra arguments for the manager.
    """
    os.makedirs(path, exist_ok=True)
    options: dict[str, Any] = {
        'preset': iroh.preset_minimal(),
        'relay_mode': iroh.RelayMode.disabled(),
        'bind_addr': '127.0.0.1:0',
        'online_timeout': None,
        **kwargs,
    }
    return PeerManager(
        SecretKey.generate(),
        Allowlist(EndpointDir(path).peers_path),
        **options,
    )


def allow_peer(manager: PeerManager, peer: PeerManager, name: str) -> None:
    """Add a peer to the allowlist of a started manager."""
    endpoint_dir = EndpointDir(os.path.dirname(manager._allowlist.path))
    peers = endpoint_dir.read_peers()
    endpoint_dir.write_peers(
        PeersConfig(peers={**peers.peers, name: peer.id}),
    )


def connect_peers(*managers: PeerManager) -> None:
    """Allow started managers to communicate with each other."""
    for i, manager in enumerate(managers):
        for j, peer in enumerate(managers):
            if i != j:
                allow_peer(manager, peer, f'peer-{j}')
                manager.add_peer_addr(peer.addr())
