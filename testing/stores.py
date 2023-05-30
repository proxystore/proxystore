"""Mocking utilities for Store tests."""
from __future__ import annotations

import uuid
from typing import Generator

import pytest

from proxystore.connectors.local import LocalConnector
from proxystore.store import Store
from proxystore.store import store_registration


@pytest.fixture()
def store() -> Generator[Store[LocalConnector], None, None]:
    """Fixture which yields a store suitable for testing.

    The yielded store is initialized with a LocalConnector meaning that
    it is only suitable for use within a single process. The store is also
    registered and registered after the test.
    """
    with Store(str(uuid.uuid4()), LocalConnector(), cache_size=0) as store:
        with store_registration(store):
            yield store
