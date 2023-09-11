"""Peer-to-peer communication and relaying.

This module provides two main functionalities: the
[`PeerManager`][proxystore.p2p.manager.PeerManager] and
relay client/server implementations.

* The [`PeerManager`][proxystore.p2p.manager.PeerManager] enables
  easy communication between arbitrary peers even if peers are behind separate
  NATs. Peer connections are established using
  [aiortc](https://aiortc.readthedocs.io/){target=_blank}, an asyncio WebRTC
  implementation.
* The [`proxystore.p2p.relay`][proxystore.p2p.relay] module provides
  implementations of the relay server and associated clients that are used by
  peers to facilitate WebRTC peer connections.
"""
from __future__ import annotations
