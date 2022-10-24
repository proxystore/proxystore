"""RedisStore Unit Tests."""
from __future__ import annotations

from proxystore.store.dim.margo import MargoStore


def test_margo_store(margo_store) -> None:
    """Test RedisStore.

    All MargoStore functionality should be covered in
    tests/store/store_*_test.py.
    """
    MargoStore(margo_store.name, **margo_store.kwargs)
