"""FileStore Unit Tests."""
from __future__ import annotations

import os

from proxystore.store.file import FileStore

STORE_DIR = '/tmp/proxystore-test-8456213966545'


def test_kwargs() -> None:
    """Test FileStore kwargs."""
    store = FileStore('files', store_dir=STORE_DIR)
    assert store.kwargs['store_dir'] == STORE_DIR

    assert store._kwargs({'test': 1})['test'] == 1
    store.close()


def test_file_store_close() -> None:
    """Test FileStore Cleanup."""
    store = FileStore('files', store_dir=STORE_DIR)

    assert os.path.exists(STORE_DIR)

    store.close()

    assert not os.path.exists(STORE_DIR)
