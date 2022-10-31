"""UCXStore Unit Tests."""
from __future__ import annotations

from proxystore.store.dim.ucx import UCXStore


def test_ucx_store(ucx_store) -> None:
    """Test UCXStore.

    All UCXStore functionality should be covered in
    tests/store/store_*_test.py.
    """
    UCXStore(ucx_store.name, **ucx_store.kwargs)
