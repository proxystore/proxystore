from __future__ import annotations

import os
import pathlib
import tempfile
from unittest import mock

from proxystore.connectors.file import FileConnector


def test_close_clears_by_default(tmp_path: pathlib.Path) -> None:
    connector = FileConnector(store_dir=tmp_path)

    assert os.path.exists(tmp_path)
    assert connector.close() is True
    assert not os.path.exists(tmp_path)


def test_close_override_default(tmp_path: pathlib.Path) -> None:
    connector = FileConnector(store_dir=tmp_path, clear=True)

    assert os.path.exists(tmp_path)
    assert connector.close(clear=False) is False
    assert os.path.exists(tmp_path)


def test_multiple_closed_connectors(tmp_path: pathlib.Path) -> None:
    connector1 = FileConnector(store_dir=tmp_path)
    connector2 = FileConnector(store_dir=tmp_path)

    assert os.path.exists(tmp_path)
    connector1.close(clear=True)
    connector2.close(clear=True)
    assert not os.path.exists(tmp_path)


def test_paths_work_after_cwd_change(tmp_path: pathlib.Path) -> None:
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


def test_store_dir_path(tmp_path: pathlib.Path) -> None:
    store_dir = tmp_path / 'store'
    with FileConnector(store_dir) as connector:
        assert os.path.isdir(store_dir)
        # Paths are converted to str so the config is serializable.
        assert connector.config()['store_dir'] == str(store_dir)


def test_get_after_data_removed(tmp_path: pathlib.Path) -> None:
    with FileConnector(store_dir=tmp_path) as connector:
        key = connector.put(b'value')
        # Simulate a concurrent evict() removing the data file after get()
        # has checked that the marker exists.
        os.remove(tmp_path / key.filename)
        assert connector.get(key) is None


def test_evict_removes_marker_first(tmp_path: pathlib.Path) -> None:
    with FileConnector(store_dir=tmp_path) as connector:
        key = connector.put(b'value')
        with mock.patch('os.remove', wraps=os.remove) as mock_remove:
            connector.evict(key)
        removed = [
            os.path.basename(c.args[0]) for c in mock_remove.call_args_list
        ]
        assert removed == [f'{key.filename}.ready', key.filename]
        assert not connector.exists(key)


def test_evict_partially_removed(tmp_path: pathlib.Path) -> None:
    with FileConnector(store_dir=tmp_path) as connector:
        key = connector.put(b'value')
        # Simulate a concurrent evict() removing one of the files between
        # this evict() checking for and removing it.
        os.remove(tmp_path / f'{key.filename}.ready')
        with mock.patch('os.path.exists', return_value=True):
            connector.evict(key)
        assert not os.path.exists(tmp_path / key.filename)
