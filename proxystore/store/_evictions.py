"""Evict the objects of deleted owned proxies in a background thread.

[`OwnedProxy.__del__`][proxystore.store.ref.OwnedProxy] cannot evict the
object itself. The garbage collector can call `__del__` at any time, such
as while the same thread holds a lock of the store registry or of a store
which evicting needs, and the thread would then wait on itself forever.
Instead, `__del__` puts the factory of the proxy in a queue, which takes no
lock that the thread could hold, and a background thread evicts it.
"""

from __future__ import annotations

import logging
import os
import queue
import threading
from typing import Any

from proxystore.store.exceptions import StoreClosedError
from proxystore.store.factory import StoreFactory

logger = logging.getLogger(__name__)


class _Evictions:
    def __init__(self) -> None:
        # SimpleQueue.put() is reentrant so it can be called by __del__ while
        # this thread is in the middle of another put() or get().
        self._queue: queue.SimpleQueue[
            StoreFactory[Any, Any] | threading.Event
        ] = queue.SimpleQueue()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the background thread if it is not running.

        Starting a thread takes locks, so this must not be called by
        `__del__`.
        """
        if self._thread is not None:
            return
        with self._lock:
            if self._thread is None:  # pragma: no branch
                thread = threading.Thread(
                    target=self._run,
                    name='proxystore-evictions',
                    daemon=True,
                )
                thread.start()
                self._thread = thread

    def put(self, factory: StoreFactory[Any, Any]) -> None:
        """Evict the object of the factory later.

        This is safe to call from `__del__`.
        """
        self._queue.put(factory)

    def wait(self) -> None:
        """Wait until every eviction put before this call is done.

        If the background thread is not running, such as in a forked child
        process, the evictions are done by this thread instead.
        """
        thread = self._thread
        if thread is threading.current_thread():
            # Called while evicting (e.g., the registry closed a store it
            # made), so waiting would wait on this thread.
            return
        if thread is None:
            self._drain()
            return
        done = threading.Event()
        self._queue.put(done)
        done.wait()

    def _drain(self) -> None:
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                return
            self._handle(item)

    def _run(self) -> None:
        while True:
            self._handle(self._queue.get())

    def _handle(self, item: StoreFactory[Any, Any] | threading.Event) -> None:
        if isinstance(item, threading.Event):
            item.set()
            return
        try:
            store = item.get_store()
        except StoreClosedError:
            # The owner of the store already removed all of its objects.
            return
        except Exception:
            logger.exception('Failed to get the store to evict %s', item.key)
            return
        try:
            store.evict(item.key)
        except Exception:
            logger.exception('Failed to evict %s', item.key)

    def _reset_after_fork(self) -> None:
        # The thread does not exist in the child, and the queue may hold
        # evictions of the parent, which the parent does itself.
        running = self._thread is not None
        self._queue = queue.SimpleQueue()
        self._lock = threading.Lock()
        self._thread = None
        if running:
            self.start()


evictions = _Evictions()

if hasattr(os, 'register_at_fork'):  # pragma: no branch
    os.register_at_fork(after_in_child=evictions._reset_after_fork)
