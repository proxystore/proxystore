"""FileStore Unit Tests."""
from __future__ import annotations

import os
import pathlib
import tempfile

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

    with tempfile.TemporaryDirectory() as tmp_dir:
        os.chdir(tmp_dir)

        # relative to tmp_dir
        store_dir = './store-dir'
        new_working_dir = os.path.join(tmp_dir, 'new-working-dir')
        os.makedirs(new_working_dir, exist_ok=True)

        with FileStore('store', store_dir=store_dir, cache_size=0) as store:
            key = store.set('data')
            os.chdir(new_working_dir)
            assert store.get(key) == 'data'

    os.chdir(current)
