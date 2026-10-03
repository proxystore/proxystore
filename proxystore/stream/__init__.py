"""Streaming interface.

Tip:
    Checkout the [Streaming Guide](../../guides/streaming.md) to learn more!
"""

from __future__ import annotations

from proxystore.stream._consumer import StreamConsumer
from proxystore.stream._producer import StreamProducer

__all__ = ['StreamConsumer', 'StreamProducer']
