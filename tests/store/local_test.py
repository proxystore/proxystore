"""LocalStore Unit Tests."""
from __future__ import annotations

from proxystore.store.local import LocalStore


def test_kwargs(local_store) -> None:
    """Test LocalStore kwargs."""
    store = LocalStore('local', **local_store.kwargs)
    assert store.kwargs['store_dict'] == local_store.kwargs['store_dict']

    assert store._kwargs({'test': 1})['test'] == 1
    store.close()
