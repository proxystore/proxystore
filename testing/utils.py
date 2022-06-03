"""Fixtures and utilities for testing."""
from __future__ import annotations

import os
import shutil
import socket
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


def open_port() -> int:
    """Return open port.

    Source: https://stackoverflow.com/questions/2838244
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    return port
