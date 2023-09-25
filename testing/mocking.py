"""Mocking utilities."""
from __future__ import annotations

from typing import Any
from typing import Callable
from unittest.mock import AsyncMock


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
