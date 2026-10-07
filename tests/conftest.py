from __future__ import annotations

import asyncio
import platform
import sys
from collections.abc import Generator
from unittest import mock

import pytest
import uvloop

from proxystore.store.registry import registry
from proxystore.store.registry import StoreRegistry

# Import fixtures from testing/ so they are known by pytest
# and can be used with
from testing.connectors import connectors
from testing.connectors import daos_connector
from testing.connectors import endpoint_connector
from testing.connectors import file_connector
from testing.connectors import globus_connector
from testing.connectors import local_connector
from testing.connectors import multi_connector
from testing.connectors import redis_connector
from testing.connectors import zmq_connector
from testing.endpoint import endpoint
from testing.endpoint import endpoint_dir
from testing.mocked import pydaos as mocked_pydaos
from testing.stores import store

# PyDAOS is not available on PyPI so we always use the mocked version. This
# must happen before any imports of proxystore.connectors.daos.
sys.modules['pydaos'] = mocked_pydaos


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
def _guard_uvloop_run(
    use_uvloop: bool,
) -> Generator[None, None, None]:
    """Fail if ``uvloop.run()`` is called when uvloop is not requested.

    uvloop should only be used when ``--use-uvloop`` is passed to pytest.
    """
    if use_uvloop:  # pragma: no cover
        yield
        return

    with mock.patch(
        'uvloop.run',
        side_effect=RuntimeError(
            'uvloop.run() was called when --use-uvloop=False. uvloop '
            'should only be used when --use-uvloop is passed to pytest.',
        ),
    ):
        yield


@pytest.fixture(scope='session', autouse=True)
def _disable_n0_services() -> Generator[None, None, None]:
    """Prevent iroh endpoints from using n0's public relays and discovery.

    Endpoints in the test suite always connect to peers on the same host
    using known addresses so relays and discovery are never needed.
    """
    import iroh

    with mock.patch('iroh.preset_n0', iroh.preset_minimal):
        yield


@pytest.fixture(autouse=True)
def _verify_no_registered_stores() -> Generator[None, None, None]:
    yield

    if len(registry) > 0:  # pragma: no cover
        raise RuntimeError(
            'Test left at least one store registered: '
            f'{tuple(registry._stores.keys())}.',
        )


@pytest.fixture
def store_registry(monkeypatch: pytest.MonkeyPatch) -> StoreRegistry:
    """Empty the global store registry for a test and restore it after."""
    monkeypatch.setattr(registry, '_stores', {})
    monkeypatch.setattr(registry, '_create_locks', {})
    return registry
