"""Utilities for recording Store operation metrics.

See the [Performance Guide](../../guides/performance.md) to learn more about
interacting with metrics recorded for
[`Store`][proxystore.store.base.Store] operations.
"""

from __future__ import annotations

import copy
import dataclasses
import math
import threading
import time
from collections import defaultdict
from collections import OrderedDict
from collections.abc import Hashable
from collections.abc import Sequence
from typing import Any

from proxystore.proxy import Proxy
from proxystore.store.types import ConnectorKeyT
from proxystore.store.utils import get_key

KeyT = ConnectorKeyT | Sequence[ConnectorKeyT]
"""Key types supported by [`StoreMetrics`][proxystore.store.metrics.StoreMetrics]."""  # noqa: E501
DEFAULT_MAX_KEYS = 10_000
"""Default maximum number of keys to keep metrics for."""

ProxyT = Proxy[Any] | Sequence[Proxy[Any]]
"""Proxy types supported by [`StoreMetrics`][proxystore.store.metrics.StoreMetrics].

When a `ProxyT` is passed, the keys are extracted from the proxies.
"""  # noqa: E501


@dataclasses.dataclass
class TimeStats:
    """Tracks time statistics of a recurring event.

    Attributes:
        count: Number of times this event has occurred.
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
    """Record and query metrics on [`Store`][proxystore.store.base.Store] operations.

    Metrics are kept per key for the `max_keys` most recently recorded keys
    (or sequences of keys). When a key is dropped, its times are still
    included in
    [`aggregate_times()`][proxystore.store.metrics.StoreMetrics.aggregate_times],
    but [`get_metrics()`][proxystore.store.metrics.StoreMetrics.get_metrics]
    returns `None` for it.

    Args:
        max_keys: Maximum number of keys to keep metrics for.

    Raises:
        ValueError: If `max_keys` is less than zero.
    """  # noqa: E501

    def __init__(self, max_keys: int = DEFAULT_MAX_KEYS) -> None:
        if max_keys < 0:
            raise ValueError(f'max_keys must be >= 0. Got {max_keys}.')
        self.max_keys = max_keys
        # Metrics of each key, least recently recorded first.
        self._metrics: OrderedDict[Hashable, Metrics] = OrderedDict()
        # Times of the keys which were dropped.
        self._dropped_times: dict[str, TimeStats] = defaultdict(TimeStats)
        self._lock = threading.Lock()

    def _reset_after_fork(self) -> None:
        # The lock may have been held by another thread of the parent when
        # the process was forked.
        self._lock = threading.Lock()

    def _record(self, key: KeyT) -> Metrics:
        # Get the metrics of a key to record to. The caller must hold the
        # lock and call _trim() after recording.
        normalized = _normalize_key(key)
        metrics = self._metrics.get(normalized)
        if metrics is None:
            metrics = Metrics()
            self._metrics[normalized] = metrics
        else:
            self._metrics.move_to_end(normalized)
        return metrics

    def _trim(self) -> None:
        # Drop the least recently recorded keys and keep their times. The
        # caller must hold the lock.
        while len(self._metrics) > self.max_keys:
            _, dropped = self._metrics.popitem(last=False)
            for name, stats in dropped.times.items():
                self._dropped_times[name] += stats

    def add_attribute(self, name: str, key: KeyT, value: Any) -> None:
        """Add an attribute associated with the key.

        Args:
            name: Name of attribute.
            key: Key to add attribute to.
            value: Attribute value.
        """
        with self._lock:
            self._record(key).attributes[name] = value
            self._trim()

    def add_counter(self, name: str, key: KeyT, value: int) -> None:
        """Add to a counter.

        Args:
            name: Name of counter.
            key: Key associated with the counter.
            value: Amount to increment counter by.
        """
        with self._lock:
            counters = self._record(key).counters
            counters[name] = counters.get(name, 0) + value
            self._trim()

    def add_time(self, name: str, key: KeyT, time_ms: float) -> None:
        """Record a new time for an event.

        Args:
            name: Event or operation the time is for.
            key: Key associated with the event.
            time_ms: The time in milliseconds of the event.
        """
        with self._lock:
            times = self._record(key).times
            if name not in times:
                times[name] = TimeStats()
            times[name].add_time(time_ms)
            self._trim()

    def aggregate_times(self) -> dict[str, TimeStats]:
        """Aggregate time statistics over all keys.

        This includes keys which per-key metrics are no longer kept for.

        Returns:
            Dictionary mapping event names to the time statistics aggregated \
            for that event.
        """
        with self._lock:
            times: dict[str, TimeStats] = defaultdict(TimeStats)
            for name, stats in self._dropped_times.items():
                times[name] += stats
            for metrics in self._metrics.values():
                for name, stats in metrics.times.items():
                    times[name] += stats
        return times

    def get_metrics(self, key_or_proxy: KeyT | ProxyT) -> Metrics | None:
        """Get the metrics associated with a key.

        Args:
            key_or_proxy: Key to get associated metrics. If a proxy or
                sequence of proxies, the key(s) will be extracted.

        Returns:
            Metrics associated with the key or `None` if no metrics are \
            kept for the key.
        """
        key = _normalize_key(key_or_proxy)
        with self._lock:
            metrics = self._metrics.get(key)
            return None if metrics is None else copy.deepcopy(metrics)


def _normalize_key(key_or_proxy: KeyT | ProxyT) -> Hashable:
    """Get the key, or tuple of keys, of a key or proxy or sequence of them."""
    if isinstance(key_or_proxy, Proxy):
        return get_key(key_or_proxy)
    if isinstance(key_or_proxy, Sequence) and all(
        isinstance(proxy, Proxy) for proxy in key_or_proxy
    ):
        return tuple(
            get_key(proxy)  # type: ignore[arg-type]
            for proxy in key_or_proxy
        )
    if isinstance(key_or_proxy, tuple):
        return key_or_proxy
    return tuple(key_or_proxy)


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
