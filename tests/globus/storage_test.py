from __future__ import annotations

import pathlib
from unittest import mock

from globus_sdk.tokenstorage import SQLiteAdapter

from proxystore.globus.storage import get_token_storage_adapter


def test_get_token_storage_adapter() -> None:
    adapter = get_token_storage_adapter(':memory:')
    assert isinstance(adapter, SQLiteAdapter)


def test_get_token_storage_adapter_proxystore_default(
    tmp_path: pathlib.Path,
) -> None:
    parent_dir = tmp_path / 'storage'
    with mock.patch(
        'proxystore.globus.storage.home_dir',
        return_value=str(parent_dir),
    ):
        adapter = get_token_storage_adapter()

    assert isinstance(adapter, SQLiteAdapter)
    assert parent_dir.exists()
    assert len(list(parent_dir.glob('*.db'))) == 1
