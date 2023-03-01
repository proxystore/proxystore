"""ZMQConnector Unit Tests."""
from __future__ import annotations

import asyncio

import pytest

from proxystore.connectors.dim.zmq import wait_for_server
from testing.utils import open_port


def test_wait_for_server() -> None:
    with pytest.raises(RuntimeError, match='timeout'):
        asyncio.run(wait_for_server('localhost', open_port(), timeout=0.01))
