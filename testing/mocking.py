"""Mocking utilities."""
from __future__ import annotations

import contextlib
import sys
from typing import Any
from typing import Callable
from typing import Generator
from unittest import mock

if sys.version_info >= (3, 8):  # pragma: >=3.8 cover
    from unittest.mock import AsyncMock
else:  # pragma: <3.8 cover
    from asynctest import CoroutineMock as AsyncMock


def async_mock_once(
    func: Callable[..., Any],
    return_value: Any,
) -> AsyncMock:
    """Mock async function once then call function normally."""
    amock = AsyncMock()

    async def return_once(*args: list[Any], **kwargs: dict[str, Any]) -> Any:
        if amock.await_count > 1:
            return await func(*args, **kwargs)
        return return_value

    amock.side_effect = return_once

    return amock


@contextlib.contextmanager
def mock_multiprocessing() -> Generator[None, None, None]:
    """Mock Process.{start,join,terminate}."""
    with mock.patch('multiprocessing.Process.start'), mock.patch(
        'multiprocessing.Process.join',
    ), mock.patch(
        'multiprocessing.Process.terminate',
    ):
        yield
