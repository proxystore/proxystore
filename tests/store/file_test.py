"""FileStore Unit Tests."""
from __future__ import annotations

import os
import pathlib

from proxystore.store.file import FileStore


def test_file_store_close(tmp_path: pathlib.Path) -> None:
    """Test FileStore Cleanup."""
    store = FileStore('files', store_dir=str(tmp_path))

    assert os.path.exists(tmp_path)

    store.close()

    assert not os.path.exists(tmp_path)


def test_cwd_change(tmp_path: pathlib.Path) -> None:
    """Checks FileStore proxies still resolve when the CWD changes."""
    current = os.getcwd()

    os.chdir(tmp_path)
    store_dir = './store-dir'

    new_working_dir = os.path.join(tmp_path, 'new-working-dir')
    os.makedirs(new_working_dir, exist_ok=True)

    with FileStore('store', store_dir=store_dir, cache_size=0) as store:
        p = store.proxy('data')
        os.chdir(new_working_dir)
        assert p == 'data'

    os.chdir(current)
