"""FileConnector Unit Tests."""
from __future__ import annotations

import os
import pathlib
import tempfile

from proxystore.connectors.file import FileConnector


def test_file_conenctor_close(tmp_path: pathlib.Path) -> None:
    """Test FileConnector Cleanup."""
    connector = FileConnector(store_dir=str(tmp_path))

    assert os.path.exists(tmp_path)

    connector.close()

    assert not os.path.exists(tmp_path)


def test_cwd_change(tmp_path: pathlib.Path) -> None:
    """Checks FileConnector still resolve when the CWD changes."""
    current = os.getcwd()

    with tempfile.TemporaryDirectory() as tmp_dir:
        os.chdir(tmp_dir)

        # relative to tmp_dir
        store_dir = './store-dir'
        new_working_dir = os.path.join(tmp_dir, 'new-working-dir')
        os.makedirs(new_working_dir, exist_ok=True)

        connector = FileConnector(store_dir=store_dir)
        key = connector.put(b'data')
        os.chdir(new_working_dir)
        assert connector.get(key) == b'data'
        connector.close()

    os.chdir(current)
