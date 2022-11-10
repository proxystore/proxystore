"""RedisStore Unit Tests."""
from __future__ import annotations

import pytest

from proxystore.store.dim.websockets import WebsocketStore


def test_websocket_store(websocket_store) -> None:
    """Test WebsocketStore.

    All WebsocketStore functionality should be covered in
    tests/store/store_*_test.py.
    """
    store = WebsocketStore(websocket_store.name, **websocket_store.kwargs)
    # starting server when already started should throw an error
    with pytest.raises(OSError):
        store._start_server()
    store.close()
    store.close()  # close when already closed doesn't do anything
