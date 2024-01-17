"""Pub/sub message stream implementations.

Warning:
    The pub/sub interfaces are experimental and may change in future
    releases.

The [`Publisher`][proxystore.pubsub.protocols.Publisher] and
[`Subscriber`][proxystore.pubsub.protocols.Subscriber] are
[`Protocols`][typing.Protocol] which define the publisher and subscriber
interfaces to a pub/sub-like messaging system.

This sub-package provides both the base protocols and implementations of the
protocol for common pub/sub systems.

Typical usage follows this pattern.

**Publisher**
```python
publisher = Publisher(...)

for message in ...:
    publisher.send(message)

producer.close()
```

**Subscriber**
```python
subscriber = Subscriber(...)

for message in subscriber:
    ...

subscriber.close()
```
"""
from __future__ import annotations

import warnings

from proxystore.warnings import ExperimentalWarning

warnings.warn(
    'MultiConnector is an experimental feature and may change in the future.',
    category=ExperimentalWarning,
    stacklevel=2,
)
