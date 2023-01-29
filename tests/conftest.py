"""Public fixtures for unit tests."""
from __future__ import annotations

import asyncio
import contextlib
import sys
from typing import Any
from typing import ContextManager
from typing import Generator
from unittest import mock

import pytest
import uvloop

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
from testing.stores import endpoint_store
from testing.stores import file_store
from testing.stores import globus_store
from testing.stores import local_store
from testing.stores import margo_store
from testing.stores import redis_store
from testing.stores import store_implementation
from testing.stores import ucx_store
from testing.stores import websocket_store


def pytest_addoption(parser):
    """Add custom command line options for tests."""
    parser.addoption(
        '--use-uvloop',
        action='store_true',
        default=False,
        help='Use uvloop as the default event loop for asyncio tests',
    )
    parser.addoption(
        '--extras',
        action='store_true',
        default=False,
        help='Run extra tests that are disable by default',
    )


@pytest.fixture(scope='session')
def use_uvloop(request) -> bool:
    """Fixture that returns if uvloop should be used in this session."""
    return request.config.getoption('--use-uvloop')


@pytest.fixture(scope='session')
def event_loop(
    use_uvloop: bool,
) -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Get event loop.

    Share event loop between all tests. Necessary for session scoped asyncio
    fixtures.

    Source: https://github.com/pytest-dev/pytest-asyncio#event_loop
    """
    context: ContextManager[Any]
    # Note: both of these are excluded from coverage because only one will
    # execute depending on the value of --use-uvloop
    if use_uvloop:  # pragma: no cover
        uvloop.install()
        context = contextlib.nullcontext()
    else:  # pragma: no cover
        context = mock.patch(
            'uvloop.install',
            side_effect=RuntimeError(
                'uvloop.install() was called when --use-uvloop=False. uvloop '
                'should only be used when --use-uvloop is passed to pytest.',
            ),
        )

    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    with context:
        yield loop
    loop.close()
