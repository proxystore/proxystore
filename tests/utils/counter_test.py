from __future__ import annotations

import pytest

from proxystore.utils.counter import AtomicCounter


def test_counter() -> None:
    counter = AtomicCounter()

    values = [counter.increment() for _ in range(100)]
    assert len(set(values)) == len(values)

    diffs = [values[i + 1] - values[i] for i in range(len(values) - 1)]
    assert all(d == 1 for d in diffs)


def test_counter_max_size() -> None:
    counter = AtomicCounter(size=2)

    counter.increment()
    counter.increment()

    with pytest.raises(ValueError):
        counter.increment()
