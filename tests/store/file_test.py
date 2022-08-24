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


def test_cwd_change(tmp_dir: str) -> None:
    """Checks FileStore proxies still resolve when the CWD changes."""
    os.chdir(tmp_dir)
    store_dir = './store-dir'

    new_working_dir = os.path.join(tmp_dir, 'new-working-dir')
    os.makedirs(new_working_dir, exist_ok=True)

    with FileStore('store', store_dir=store_dir, cache_size=0) as store:
        p = store.proxy('data')
        os.chdir(new_working_dir)
        assert p == 'data'
