"""ZMQConnector Unit Tests."""
from __future__ import annotations

import pytest

from proxystore.connectors.dim.zmq import MAX_CHUNK_LENGTH
from proxystore.connectors.dim.zmq import wait_for_server
from proxystore.connectors.dim.zmq import ZeroMQConnector
from testing.compat import randbytes
from testing.utils import open_port


def test_wait_for_server() -> None:
    with pytest.raises(RuntimeError, match='timeout'):
        wait_for_server('localhost', open_port(), timeout=0.01)


def test_large_message_sizes(zmq_connector) -> None:
    # Note: This test will hang on its own because it doesn't call close
    # on the connector, but will work if tests/connectors/connectors_test.py
    # is also run.
    connector = ZeroMQConnector(**zmq_connector.kwargs)
    data = randbytes(3 * MAX_CHUNK_LENGTH)
    key = connector.put(data)
    assert connector.get(key) is not None
    connector.evict(key)
    # Note: Don't close connector because it will close server used by other
    # tests using the same fixture.
