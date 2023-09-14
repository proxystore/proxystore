from __future__ import annotations

import time

import pytest

from proxystore.utils.timer import Timer


def test_timer() -> None:
    timer = Timer()
    timer.start()
    time.sleep(0.001)
    timer.stop()

    assert timer.elapsed_ns > 1000
    assert timer.elapsed_ms > 1
    assert timer.elapsed_s > 0.001


def test_timer_context_manger() -> None:
    with Timer() as timer:
        time.sleep(0.001)

    assert timer.elapsed_ns > 1000
    assert timer.elapsed_ms > 1
    assert timer.elapsed_s > 0.001


def test_timer_still_running() -> None:
    with Timer() as timer:
        with pytest.raises(RuntimeError, match='Timer is still running!'):
            timer.elapsed_ns  # noqa: B018

        with pytest.raises(RuntimeError, match='Timer is still running!'):
            timer.elapsed_ms  # noqa: B018

        with pytest.raises(RuntimeError, match='Timer is still running!'):
            timer.elapsed_s  # noqa: B018
