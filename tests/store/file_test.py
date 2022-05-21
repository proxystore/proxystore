"""FileStore Unit Tests."""
from __future__ import annotations

import os
import uuid

from proxystore.store.file import FileStore


def test_file_store_close() -> None:
    """Test FileStore Cleanup."""
    store_dir = f'/tmp/proxystore-test-{uuid.uuid4()}'
    store = FileStore('files', store_dir=store_dir)

    assert os.path.exists(store_dir)

    store.close()

    assert not os.path.exists(store_dir)
