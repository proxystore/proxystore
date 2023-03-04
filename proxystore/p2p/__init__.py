"""Peer-to-peer communication and relaying.

This module provides two main functionalities: the
[`PeerManager`][proxystore.p2p.manager.PeerManager] and
[`RelayServer`][proxystore.p2p.relay.RelayServer].

* The [`PeerManager`][proxystore.p2p.manager.PeerManager] enables
  easy communication between arbitrary peers even if peers are behind separate
  NATs. Peer connections are established using
  [aiortc](https://aiortc.readthedocs.io/){target=_blank}, an asyncio WebRTC
  implementation.
* The [`RelayServer`][proxystore.p2p.relay.RelayServer] is a
  commonly accessible server by peers that is used to facilitate WebRTC peer
  connections.
"""
from __future__ import annotations
