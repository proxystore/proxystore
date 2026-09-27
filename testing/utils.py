"""Fixtures and utilities for testing."""

from __future__ import annotations

import asyncio
import socket
from collections.abc import Callable

_used_ports: set[int] = set()


def open_port() -> int:
    """Return open port.

    Source: https://stackoverflow.com/questions/2838244
    """
    while True:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('', 0))
        s.listen(1)
        port = s.getsockname()[1]
        s.close()
        if port not in _used_ports:  # pragma: no branch
            _used_ports.add(port)
            return port


async def wait_until(
    predicate: Callable[[], bool],
    *,
    interval: float = 0.01,
) -> None:
    """Wait until a condition is true.

    This waits indefinitely so tests rely on the pytest timeout. Use this
    instead of a loop in a test so the coverage of the test does not depend
    on whether the condition is true the first time it is checked.
    """
    while not predicate():
        await asyncio.sleep(interval)
