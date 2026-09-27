"""Utilities for testing peer managers."""

from __future__ import annotations

from typing import Any

import iroh

from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.identity import SecretKey
from proxystore.endpoint.p2p.manager import PeerManager
from proxystore.endpoint.p2p.manager import PeerOptions

LOCAL_PEER_OPTIONS = PeerOptions(
    preset=iroh.preset_minimal(),
    bind_addr='127.0.0.1:0',
    online_timeout=None,
)
"""Peer options which only use localhost (no relays or discovery)."""


class StaticPolicy:
    """Peer policy which allows the peers in a mapping of IDs to names.

    Tests can change the mapping to allow or deny peers.
    """

    def __init__(self) -> None:
        self.peers: dict[EndpointId, str] = {}

    def allowed(self, peer_id: EndpointId) -> bool:
        """Check if the peer is allowed."""
        return peer_id in self.peers

    def name_of(self, peer_id: EndpointId) -> str | None:
        """Get the name of the peer."""
        return self.peers.get(peer_id)


def local_peer_manager(**kwargs: Any) -> PeerManager:
    """Create a peer manager with a new key which only uses localhost.

    The manager does not use relays or discovery so peers must be added with
    [`connect_peers()`][testing.p2p.connect_peers]. The policy of the
    manager is a [`StaticPolicy`][testing.p2p.StaticPolicy] which allows no
    peers.

    Args:
        kwargs: Keyword arguments which override the defaults of the
            manager.
    """
    options: dict[str, Any] = {'options': LOCAL_PEER_OPTIONS, **kwargs}
    return PeerManager(SecretKey.generate(), StaticPolicy(), **options)


def policy(manager: PeerManager) -> StaticPolicy:
    """Get the policy of a manager created by `local_peer_manager()`."""
    assert isinstance(manager.policy, StaticPolicy)
    return manager.policy


def allow_peer(manager: PeerManager, peer: PeerManager, name: str) -> None:
    """Allow a peer to communicate with a manager."""
    policy(manager).peers[peer.id] = name


def connect_peers(*managers: PeerManager) -> None:
    """Allow started managers to communicate with each other."""
    for i, manager in enumerate(managers):
        for j, peer in enumerate(managers):
            if i != j:
                allow_peer(manager, peer, f'peer-{peer.id.short()}')
                manager.add_peer_addr(peer.addr())
