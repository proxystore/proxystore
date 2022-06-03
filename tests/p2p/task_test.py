from __future__ import annotations

import asyncio
import contextlib

import pytest

from proxystore.p2p.task import SafeTaskExit
from proxystore.p2p.task import spawn_guarded_background_task


def test_background_task_exits_on_error() -> None:
    async def okay_task() -> None:
        return

    async def safe_task() -> None:
        raise SafeTaskExit()

    async def bad_task() -> None:
        raise RuntimeError()

    async def run(task) -> None:
        await spawn_guarded_background_task(task)

    with contextlib.redirect_stdout(
        None,
    ), contextlib.redirect_stderr(None):
        asyncio.run(run(okay_task))
        with pytest.raises(SafeTaskExit):
            asyncio.run(run(safe_task))
        with pytest.raises(SystemExit):
            asyncio.run(run(bad_task))
