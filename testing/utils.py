"""Fixtures and utilities for testing."""
from __future__ import annotations

import pathlib
import socket
from typing import Generator

import pytest


@pytest.fixture()
def tmp_dir(tmp_path: pathlib.Path) -> Generator[str, None, None]:
    """Yields unique path to directory and cleans up after."""
    yield str(tmp_path)


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
