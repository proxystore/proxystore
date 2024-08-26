from __future__ import annotations

import os
import pathlib
import uuid
from unittest import mock

from proxystore.globus.app import get_token_storage
from proxystore.globus.app import is_client_login
from proxystore.globus.app import PROXYSTORE_GLOBUS_CLIENT_ID_ENV_NAME
from proxystore.globus.app import PROXYSTORE_GLOBUS_CLIENT_SECRET_ENV_NAME


def test_get_token_storage(tmp_path: pathlib.Path):
    filepath = tmp_path / 'tokens.db'
    storage = get_token_storage(filepath)
    assert filepath.is_file()
    storage.close()


def test_get_token_storage_adapter_proxystore_default(
    tmp_path: pathlib.Path,
) -> None:
    parent_dir = tmp_path / 'storage'
    with mock.patch(
        'proxystore.globus.storage.home_dir',
        return_value=str(parent_dir),
    ):
        storage = get_token_storage()

    assert parent_dir.exists()
    assert len(list(parent_dir.glob('*.db'))) == 1
    storage.close()


def test_is_client_login() -> None:
    with mock.patch.dict(os.environ, {}):
        assert not is_client_login()

    env = {PROXYSTORE_GLOBUS_CLIENT_ID_ENV_NAME: str(uuid.uuid4())}
    with mock.patch.dict(os.environ, env):
        assert not is_client_login()

    env[PROXYSTORE_GLOBUS_CLIENT_SECRET_ENV_NAME] = 'secret'
    with mock.patch.dict(os.environ, env):
        assert is_client_login()
