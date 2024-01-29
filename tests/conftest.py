from __future__ import annotations

import asyncio
import contextlib
import platform
import sys
from typing import Any
from typing import ContextManager
from typing import Generator
from unittest import mock

import pytest
import uvloop

# Import fixtures from testing/ so they are known by pytest
# and can be used with
from testing.connectors import connectors
from testing.connectors import endpoint_connector
from testing.connectors import file_connector
from testing.connectors import globus_connector
from testing.connectors import local_connector
from testing.connectors import multi_connector
from testing.connectors import redis_connector
from testing.endpoint import endpoint
from testing.relay_server import relay_server
from testing.ssl import ssl_context
from testing.stores import store


def pytest_addoption(parser):
    """Add custom command line options for tests."""
    parser.addoption(
        '--use-uvloop',
        action='store_true',
        default=False,
        help='Use uvloop as the default event loop for asyncio tests',
    )


@pytest.fixture(scope='session')
def use_uvloop(request) -> bool:
    """Fixture that returns if uvloop should be used in this session."""
    return request.config.getoption('--use-uvloop')


@pytest.fixture(scope='session')
def event_loop_policy(
    use_uvloop: bool,
) -> Generator[asyncio.AbstractEventLoopPolicy, None, None]:
    """Get the session-wide event loop policy.

    This enables us to toggle between uvloop and asyncio.
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
    with context:
        yield policy
