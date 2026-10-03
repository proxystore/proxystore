"""Mocking utilities for Store tests."""

from __future__ import annotations

from collections.abc import Generator

import pytest

from proxystore.connectors.local import LocalConnector
from proxystore.store import Store


@pytest.fixture
def store() -> Generator[Store[LocalConnector], None, None]:
    """Fixture which yields a store suitable for testing.

    The yielded store is initialized with a LocalConnector meaning that
    it is only suitable for use within a single process.
    """
    with Store(LocalConnector(), cache_size=0) as store:
        yield store
