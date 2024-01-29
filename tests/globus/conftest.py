from __future__ import annotations

import pathlib
from typing import Generator
from unittest import mock

import pytest


@pytest.fixture(autouse=True)
def _patch_storage_dir(tmp_path: pathlib.Path) -> Generator[None, None, None]:
    with mock.patch(
        'proxystore.globus.storage.home_dir',
        return_value=str(tmp_path / 'storage.db'),
    ):
        yield
