"""Peer-to-peer connection utilities and services.

This module provides two main functionalities: the
:any:`PeerManager <proxystore.p2p.manager.PeerManager>` and
:any:`SignalingServer <proxystore.p2p.server.SignalingServer>`.
The :any:`PeerManager <proxystore.p2p.manager.PeerManager>` enables
easy communication between arbitrary peers even if peers are behind separate
NATs. Peer connections are established using
`aiortc <https://aiortc.readthedocs.io/>`_, an asyncio WebRTC implementation.
The :any:`SignalingServer <proxystore.p2p.server.SignalingServer>` is a
commonly accessible server by peers that is used to facilitate WebRTC peer
connections.
"""
from __future__ import annotations
