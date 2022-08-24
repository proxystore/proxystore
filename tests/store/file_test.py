"""FileStore Unit Tests."""
from __future__ import annotations

import os

from proxystore.store.file import FileStore


def test_file_store_close(tmp_dir: str) -> None:
    """Test FileStore Cleanup."""
    store = FileStore('files', store_dir=tmp_dir)

    assert os.path.exists(tmp_dir)

    store.close()

    assert not os.path.exists(tmp_dir)
