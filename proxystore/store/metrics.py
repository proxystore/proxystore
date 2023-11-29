"""Utilities for recording Store operation metrics.

See the [Performance Guide](../../guides/performance.md) to learn more about
interacting with metrics recorded for
[`Store`][proxystore.store.base.Store] operations.
"""
from __future__ import annotations

import copy
import dataclasses
import math
import sys
import time
from collections import defaultdict
from typing import Any
from typing import Sequence
from typing import Tuple
from typing import Union

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    pass
else:  # pragma: <3.11 cover
    pass

from proxystore.proxy import Proxy
from proxystore.store.utils import get_key

ConnectorKeyT = Tuple[Any, ...]
KeyT = Union[ConnectorKeyT, Sequence[ConnectorKeyT]]
"""Key types supported by [`StoreMetrics`][proxystore.store.metrics.StoreMetrics]."""  # noqa: E501
ProxyT = Union[Proxy[Any], Sequence[Proxy[Any]]]
"""Proxy types supported by [`StoreMetrics`][proxystore.store.metrics.StoreMetrics].

When a `ProxyT` is passed, the keys are extracted from the proxies.
"""  # noqa: E501


@dataclasses.dataclass
class TimeStats:
    """Tracks time statistics of a reoccuring event.

    Attributes:
        count: Number of times this event as occurred.
        avg_time_ms: Average time in milliseconds of the event.
        min_time_ms: Minimum time in milliseconds of all event occurrences.
        max_time_ms: Maximum time in milliseconds of all event occurrences.
        last_time_ms: Time in milliseconds of the most recent event occurrence.
        last_timestamp: The UNIX timestamp (seconds) of when the last
            event time was recorded.
    """

    count: int = 0
    avg_time_ms: float = 0
    min_time_ms: float = math.inf
    max_time_ms: float = 0
    last_time_ms: float = 0
    last_timestamp: float = 0

    def __add__(self, other: TimeStats) -> TimeStats:
        if self.last_timestamp > other.last_timestamp:
            last_time_ms = self.last_time_ms
            last_timestamp = self.last_timestamp
        else:
            last_time_ms = other.last_time_ms
            last_timestamp = other.last_timestamp

        return TimeStats(
            count=self.count + other.count,
            avg_time_ms=_weighted_avg(
                self.avg_time_ms,
                self.count,
                other.avg_time_ms,
                other.count,
            ),
            min_time_ms=min(self.min_time_ms, other.min_time_ms),
            max_time_ms=max(self.max_time_ms, other.max_time_ms),
            last_time_ms=last_time_ms,
            last_timestamp=last_timestamp,
        )

    def add_time(self, time_ms: float) -> None:
        self.avg_time_ms = _weighted_avg(
            self.avg_time_ms,
            self.count,
            time_ms,
            1,
        )
        self.max_time_ms = max(self.max_time_ms, time_ms)
        self.min_time_ms = min(self.min_time_ms, time_ms)
        self.last_time_ms = time_ms
        self.last_timestamp = time.time()
        self.count += 1

    def as_dict(self) -> dict[str, Any]:
        """Convert the dataclass to a [`dict`][dict]."""
        return dataclasses.asdict(self)


@dataclasses.dataclass
class Metrics:
    """Records metrics and attributes for events.

    Attributes:
        attributes: A mapping of attributes to their values.
        counters: A mapping of counter names to the integer value of the
            counter.
        times: A mapping of events to a summary of the statistics recorded
            over occurrences of that event.
    """

    attributes: dict[str, Any] = dataclasses.field(default_factory=dict)
    counters: dict[str, int] = dataclasses.field(default_factory=dict)
    times: dict[str, TimeStats] = dataclasses.field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        """Convert the dataclass to a [`dict`][dict]."""
        return dataclasses.asdict(self)


class StoreMetrics:
    """Record and query metrics on [`Store`][proxystore.store.base.Store] operations."""  # noqa: E501

    def __init__(self) -> None:
        self._metrics: dict[int, Metrics] = defaultdict(Metrics)

    def add_attribute(self, name: str, key: KeyT, value: Any) -> None:
        """Add an attribute associated with the key.

        Args:
            name: Name of attribute.
            key: Key to add attribute to.
            value: Attribute value.
        """
        self._metrics[_hash_key(key)].attributes[name] = value

    def add_counter(self, name: str, key: KeyT, value: int) -> None:
        """Add to a counter.

        Args:
            name: Name of counter.
            key: Key associated with the counter.
            value: Amount to increment counter by.
        """
        counters = self._metrics[_hash_key(key)].counters
        if name in counters:
            counters[name] += value
        else:
            counters[name] = value

    def add_time(self, name: str, key: KeyT, time_ns: int) -> None:
        """Record a new time for an event.

        Args:
            name: Event or operation the time is for.
            key: Key associated with the event.
            time_ns: The time in nanoseconds of the event.
        """
        times = self._metrics[_hash_key(key)].times
        if name not in times:
            times[name] = TimeStats()
        times[name].add_time(time_ns / 1000)

    def aggregate_times(self) -> dict[str, TimeStats]:
        """Aggregate time statistics over all keys.

        Returns:
            Dictionary mapping event names to the time statistics aggregated \
            for that event.
        """
        times: dict[str, TimeStats] = defaultdict(TimeStats)
        for metrics in self._metrics.values():
            for key, value in metrics.times.items():
                times[key] += value
        return times

    def get_metrics(self, key_or_proxy: KeyT | ProxyT) -> Metrics | None:
        """Get the metrics associated with a key.

        Args:
            key_or_proxy: Key to get associated metrics. If a proxy or
                sequence of proxies, the key(s) will be extracted.

        Returns:
            Metrics associated with the key or `None` if the key does not \
            exist.
        """
        key_hash = _hash_key(key_or_proxy)
        if key_hash in self._metrics:
            return copy.deepcopy(self._metrics[key_hash])
        return None


def _hash_key(key_or_proxy: KeyT | ProxyT) -> int:
    """Hashes a Store key or sequences of keys (or proxies)."""
    key: KeyT
    if isinstance(key_or_proxy, Proxy):
        key = get_key(key_or_proxy)
    elif isinstance(key_or_proxy, Sequence) and all(
        isinstance(proxy, Proxy) for proxy in key_or_proxy
    ):
        key = tuple(
            get_key(proxy)  # type: ignore[arg-type]
            for proxy in key_or_proxy
        )
    else:
        key = key_or_proxy

    if isinstance(key, Sequence) and not isinstance(key, tuple):
        return hash(tuple(key))
    else:
        return hash(key)


def _weighted_avg(a1: float, n1: int, a2: float, n2: float) -> float:
    """Compute weighted average between two separate averages.

    Args:
        a1: The first average.
        n1: The number of samples in `a1`.
        a2: The second average.
        n2: The number of samples in `a2`.

    Returns:
        The weighted average between `a1` and `a2`.
    """
    return ((a1 * n1) + (a2 * n2)) / (n1 + n2)
