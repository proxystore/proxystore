"""RedisStore Unit Tests."""
from __future__ import annotations

import asyncio

import pytest
import zmq

from proxystore.store.dim.zmq import wait_for_server
from proxystore.store.dim.zmq import ZeroMQStore
from testing.utils import open_port


def test_zero_store(zmq_store) -> None:
    """Test ZeroMQStore.

    All ZeroMQStore functionality should be covered in
    tests/store/store_*_test.py.
    """
    store = ZeroMQStore(zmq_store.name, **zmq_store.kwargs)

    # starting server when already started should throw an error
    with pytest.raises(zmq.error.ZMQError):
        store._start_server()


def test_wait_for_server() -> None:
    with pytest.raises(RuntimeError, match='timeout'):
        asyncio.run(wait_for_server('localhost', open_port(), timeout=0.01))
