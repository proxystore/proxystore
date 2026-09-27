from __future__ import annotations

from testing.utils import wait_until


async def test_wait_until() -> None:
    calls = 0

    def _predicate() -> bool:
        nonlocal calls
        calls += 1
        return calls == 3

    await wait_until(_predicate, interval=0)
    assert calls == 3

    await wait_until(lambda: True)
