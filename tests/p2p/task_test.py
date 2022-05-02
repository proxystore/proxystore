from __future__ import annotations

import asyncio
import contextlib

import pytest

from proxystore.p2p.task import spawn_guarded_background_task


def test_background_task_exits_on_error() -> None:
    async def run() -> None:
        # Prevent pytest logs from being clobbered
        async def okay_task() -> None:
            return

        async def bad_task() -> None:
            raise RuntimeError()

        t = spawn_guarded_background_task(okay_task)
        await t
        t = spawn_guarded_background_task(bad_task)
        await t

    with pytest.raises(SystemExit), contextlib.redirect_stdout(
        None,
    ), contextlib.redirect_stderr(None):
        asyncio.run(run())
