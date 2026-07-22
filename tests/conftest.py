from __future__ import annotations

import asyncio
import platform
import sys
from collections.abc import Generator
from unittest import mock

import pytest
import uvloop

import proxystore

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


def pytest_asyncio_loop_factories(
    config: pytest.Config,
    item: pytest.Item,
) -> dict[str, object]:
    """Select the event loop factory for pytest-asyncio tests.

    This enables us to toggle between uvloop and asyncio via the
    ``--use-uvloop`` option. Returning a single-entry mapping means each
    async test runs once on the selected loop rather than being
    parametrized across every available factory.

    Replaces overriding the deprecated ``event_loop_policy`` fixture, which
    relied on ``asyncio.get_event_loop_policy()`` (removed in Python 3.16).
    """
    # Note: both branches are excluded from coverage because only one will
    # execute depending on the value of --use-uvloop.
    if config.getoption('--use-uvloop'):  # pragma: no cover
        return {'uvloop': uvloop.new_event_loop}
    return {'asyncio': asyncio.new_event_loop}  # pragma: no cover


@pytest.fixture(autouse=True)
def _guard_uvloop_install(
    use_uvloop: bool,
) -> Generator[None, None, None]:
    """Fail if ``uvloop.install()`` is called when uvloop is not requested.

    uvloop should only be used when ``--use-uvloop`` is passed to pytest.
    """
    if use_uvloop:  # pragma: no cover
        yield
        return

    with mock.patch(
        'uvloop.install',
        side_effect=RuntimeError(
            'uvloop.install() was called when --use-uvloop=False. uvloop '
            'should only be used when --use-uvloop is passed to pytest.',
        ),
    ):
        yield


@pytest.fixture(scope='session', autouse=True)
def _disable_ice_servers() -> Generator[None, None, None]:
    """Disable STUN servers when gathering ICE candidates.

    Peers created in the test suite are always on the same host so host
    candidates are sufficient to establish a connection and server-reflexive
    candidates are never used. Gathering them is not merely wasted work: a
    local address which cannot route to the STUN server stalls candidate
    gathering for five seconds because aioice does not support trickle ICE
    and waits for every STUN request to time out. WSL, for example, assigns
    a non-routable address to the loopback interface, causing every peer
    connection to take an additional five seconds to open (#599).
    """
    with mock.patch(
        'aiortc.rtcicetransport.RTCIceGatherer.getDefaultIceServers',
        return_value=[],
    ):
        yield


@pytest.fixture(autouse=True)
def _verify_no_registered_stores() -> Generator[None, None, None]:
    yield

    if len(proxystore.store._stores) > 0:  # pragma: no cover
        raise RuntimeError(
            'Test left at least one store registered: '
            f'{tuple(proxystore.store._stores.keys())}.',
        )
