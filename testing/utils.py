"""Fixtures and utilities for testing."""
from __future__ import annotations

import os
import shutil
import uuid
from typing import Generator

import pytest


@pytest.fixture()
def tmp_dir() -> Generator[str, None, None]:
    """Yields unique path to directory and cleans up after."""
    path = f'/tmp/{uuid.uuid4()}'
    yield path
    if os.path.exists(path):
        shutil.rmtree(path)
