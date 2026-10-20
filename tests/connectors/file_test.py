from __future__ import annotations

import contextlib
import logging
import os
import pathlib
import stat
from unittest import mock

import pytest

from proxystore.connectors.file import FileConnector


def test_close_clears_by_default(tmp_path: pathlib.Path) -> None:
    store_dir = tmp_path / 'store'
    connector = FileConnector(store_dir=store_dir)

    assert os.path.exists(store_dir)
    assert connector.close() is True
    assert not os.path.exists(store_dir)


def test_close_override_default(tmp_path: pathlib.Path) -> None:
    store_dir = tmp_path / 'store'
    connector = FileConnector(store_dir=store_dir, clear=True)

    assert os.path.exists(store_dir)
    assert connector.close(clear=False) is False
    assert os.path.exists(store_dir)


def test_multiple_closed_connectors(
    tmp_path: pathlib.Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    store_dir = tmp_path / 'store'
    connector1 = FileConnector(store_dir=store_dir)
    connector2 = FileConnector(store_dir=store_dir)

    assert os.path.exists(store_dir)
    assert connector1.close(clear=True) is True
    assert connector2.close(clear=True) is False
    assert not os.path.exists(store_dir)
    # Nothing is left to remove so the second close does not warn.
    assert caplog.text == ''


def test_close_keeps_existing_dir(
    tmp_path: pathlib.Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    user_file = tmp_path / 'data.txt'
    user_file.write_text('data')

    connector = FileConnector(store_dir=tmp_path)
    key = connector.put(b'value')
    assert connector.close(clear=False) is False
    assert caplog.text == ''

    assert connector.close() is False
    assert 'existed before the FileConnector was created' in caplog.text
    assert user_file.read_text() == 'data'
    assert (tmp_path / key.filename).exists()


def test_paths_work_after_cwd_change(tmp_path: pathlib.Path) -> None:
    new_working_dir = tmp_path / 'new-working-dir'
    new_working_dir.mkdir()

    with contextlib.chdir(tmp_path):
        # relative to tmp_path
        connector = FileConnector(store_dir='./store-dir')
        key = connector.put(b'data')
        with contextlib.chdir(new_working_dir):
            assert connector.get(key) == b'data'
            connector.close()


def test_store_dir_path(tmp_path: pathlib.Path) -> None:
    store_dir = tmp_path / 'store'
    with FileConnector(store_dir) as connector:
        assert os.path.isdir(store_dir)
        # Paths are converted to str so the config is serializable.
        assert connector.config()['store_dir'] == str(store_dir)


def test_set_writes_through_temp_file(tmp_path: pathlib.Path) -> None:
    with FileConnector(store_dir=tmp_path) as connector:
        key = connector.new_key()
        with mock.patch('os.replace', wraps=os.replace) as mock_replace:
            connector.set(key, b'value')
        src, dst = mock_replace.call_args.args
        name = os.path.basename(src)
        assert name.startswith(f'.{key.filename}.')
        assert name.endswith('.tmp')
        assert dst == str(tmp_path / key.filename)
        # Only the object's file is left in the directory.
        assert os.listdir(tmp_path) == [key.filename]
        assert connector.get(key) == b'value'

        # Setting the key again replaces the file.
        connector.set(key, b'new')
        assert connector.get(key) == b'new'
        assert os.listdir(tmp_path) == [key.filename]


def test_set_removes_temp_file_on_error(tmp_path: pathlib.Path) -> None:
    with FileConnector(store_dir=tmp_path) as connector:
        key = connector.new_key()
        with (
            mock.patch('os.replace', side_effect=OSError('failed')),
            pytest.raises(OSError, match='failed'),
        ):
            connector.set(key, b'value')
        assert os.listdir(tmp_path) == []
        assert not connector.exists(key)

        # The error is still raised if the temp file is already gone.
        with (
            mock.patch('os.replace', side_effect=OSError('failed')),
            mock.patch('os.remove', side_effect=FileNotFoundError),
            pytest.raises(OSError, match='failed'),
        ):
            connector.set(key, b'value')


def test_evict_missing(tmp_path: pathlib.Path) -> None:
    with FileConnector(store_dir=tmp_path) as connector:
        key = connector.put(b'value')
        connector.evict(key)
        assert not connector.exists(key)
        assert connector.get(key) is None
        # Should not fail, e.g., if a concurrent evict() removed it.
        connector.evict(key)


def _mode(path: str | pathlib.Path) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


def test_default_permissions(tmp_path: pathlib.Path) -> None:
    store_dir = tmp_path / 'parent' / 'store'
    with FileConnector(store_dir) as connector:
        key = connector.put(b'value')
        assert _mode(store_dir) == 0o700
        assert _mode(store_dir / key.filename) == 0o600
        assert connector.config()['dir_mode'] == 0o700
        assert connector.config()['file_mode'] == 0o600


def test_permissions_not_limited_by_umask(tmp_path: pathlib.Path) -> None:
    store_dir = tmp_path / 'store'
    old = os.umask(0o077)
    try:
        with FileConnector(
            store_dir,
            dir_mode=0o770,
            file_mode=0o660,
        ) as connector:
            key = connector.put(b'value')
            assert _mode(store_dir) == 0o770
            assert _mode(store_dir / key.filename) == 0o660

            new = FileConnector.from_config(connector.config())
            assert new.dir_mode == 0o770
            assert new.file_mode == 0o660
    finally:
        os.umask(old)


def test_existing_dir_keeps_permissions(tmp_path: pathlib.Path) -> None:
    store_dir = tmp_path / 'store'
    store_dir.mkdir()
    os.chmod(store_dir, 0o755)
    with FileConnector(store_dir):
        assert _mode(store_dir) == 0o755


def test_dir_created_by_another_process(tmp_path: pathlib.Path) -> None:
    store_dir = tmp_path / 'store'
    store_dir.mkdir()
    os.chmod(store_dir, 0o755)
    # Simulate another process creating the directory after this one
    # checked that it does not exist.
    with (
        mock.patch('os.path.exists', return_value=False),
        FileConnector(store_dir) as connector,
    ):
        assert _mode(store_dir) == 0o755
    # The other process created the directory so it is not removed.
    assert connector.close() is False
    assert os.path.isdir(store_dir)
