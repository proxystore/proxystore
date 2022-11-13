"""Public fixtures for unit tests."""
from __future__ import annotations

import asyncio
import sys
from typing import Generator

import pytest

try:
    import pymargo
except ImportError:
    from testing.mocked import pymargo

    sys.modules['pymargo'] = pymargo
    sys.modules['pymargo.bulk'] = pymargo
    sys.modules['pymargo.core'] = pymargo

try:
    import ucp
except ImportError:
    from testing.mocked import ucx

    sys.modules['ucp'] = ucx


# Import fixtures from testing/ so they are known by pytest
# and can be used with
from testing.endpoint import endpoint
from testing.signaling_server import signaling_server
from testing.store_utils import endpoint_store
from testing.store_utils import file_store
from testing.store_utils import globus_store
from testing.store_utils import local_store
from testing.store_utils import margo_store
from testing.store_utils import redis_store
from testing.store_utils import ucx_store
from testing.store_utils import websocket_store


@pytest.fixture(scope='session')
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Get event loop.

    Share event loop between all tests. Necessary for session scoped asyncio
    fixtures.

    Source: https://github.com/pytest-dev/pytest-asyncio#event_loop
    """
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()
