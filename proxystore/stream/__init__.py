"""Proxy streaming interface.

Warning:
    The streaming interfaces are experimental and may change in future
    releases.

Tip:
    Checkout the [Streaming Guide](../../guides/streaming.md) to learn more!

Note:
    The [StreamProducer][proxystore.stream.interface.StreamProducer]
    and [StreamConsumer][proxystore.stream.interface.StreamConsumer]
    are defined in [`proxystore.stream.interface`][proxystore.stream.interface]
    are re-exported here for convenience.
"""
from __future__ import annotations

import warnings

from proxystore.stream.interface import StreamConsumer
from proxystore.stream.interface import StreamProducer
from proxystore.warnings import ExperimentalWarning

warnings.warn(
    'Streaming is an experimental feature and may change in the future.',
    category=ExperimentalWarning,
    stacklevel=2,
)
