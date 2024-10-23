from __future__ import annotations

import asyncio
import sys

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import pytest

from proxystore.proxy import Proxy


@pytest.mark.asyncio
async def test_awaitable() -> None:
    event = asyncio.Event()

    async def test_coro() -> None:
        event.set()

    wrapped = Proxy(lambda: test_coro)
    assert not event.is_set()

    await wrapped()

    assert event.is_set()


@pytest.mark.asyncio
async def test_asyn_iterator() -> None:
    class TestAsyncIterator:
        def __init__(self, value: list[int]) -> None:
            self.iter = iter(values)

        def __aiter__(self) -> Self:
            return self

        async def __anext__(self) -> int:
            try:
                return next(self.iter)
            except StopIteration as e:
                raise StopAsyncIteration from e

    values = [1, 2, 3]
    target = TestAsyncIterator(values)
    wrapped = Proxy(lambda: target)

    found = []
    async for value in wrapped:  # pragma: no cover
        found.append(value)

    assert values == found


@pytest.mark.asyncio
async def test_async_context_manager() -> None:
    class TestAsyncContextManager:
        def __init__(self) -> None:
            self.entered = False
            self.exited = False

        async def __aenter__(self):
            self.entered = True

        async def __aexit__(self, exc_type, exc, tb):
            self.exited = True

    manager = TestAsyncContextManager()
    wrapped = Proxy(lambda: manager)

    assert not wrapped.entered
    assert not wrapped.exited

    async with wrapped:
        assert wrapped.entered

    assert wrapped.exited
