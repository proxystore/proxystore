"""Utilities for launching async tasks."""
from __future__ import annotations

import asyncio
import traceback
from typing import Any
from typing import Callable
from typing import Coroutine


def exit_on_error(task: asyncio.Task[Any]) -> None:
    """Task callback that raises SystemExit on task exception."""
    if not task.cancelled() and task.exception() is not None:
        e = task.exception()
        traceback.print_exception(
            None,
            e,
            e.__traceback__ if e is not None else None,
        )
        raise SystemExit(1)


def spawn_guarded_background_task(
    coro: Callable[..., Coroutine[Any, Any, None]],
    *args: Any,
    **kwargs: Any,
) -> asyncio.Task[Any]:
    """Run a coroutine safely in the background.

    Launches the coroutine as an asyncio task and sets the done
    callback to :func:`exit_on_error() <.exit_on_error()>`. This is "safe"
    because it will ensure exceptions inside the task get logged and cause
    the program to exit. Otherwise, background tasks that are not awaited
    may not have their exceptions raised such that programs hang with no
    notice of the exception that caused the hang.

    Source: `<https://stackoverflow.com/questions/62588076>`_

    Args:
        coro (Coroutine): coroutine to run as task.
        args (list): positional arguments for the coroutine.
        kwargs (dict): keyword arguments for the coroutine.

    Returns:
        asyncio task handle.
    """
    task = asyncio.create_task(coro(*args, **kwargs))
    task.add_done_callback(exit_on_error)
    return task
