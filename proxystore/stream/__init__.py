"""Streaming interface.

Warning:
    The streaming interfaces are experimental and may change in future
    releases.

Tip:
    Checkout the [Streaming Guide](../../guides/streaming.md) to learn more!
"""

from __future__ import annotations

import warnings

from proxystore.stream._consumer import StreamConsumer
from proxystore.stream._producer import StreamProducer
from proxystore.warnings import ExperimentalWarning

warnings.warn(
    'Streaming is an experimental feature and may change in the future.',
    category=ExperimentalWarning,
    stacklevel=2,
)

__all__ = ['StreamConsumer', 'StreamProducer']
