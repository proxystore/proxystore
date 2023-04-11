"""ZMQConnector Unit Tests."""
from __future__ import annotations

from unittest import mock

import pytest

from proxystore.connectors.dim.exceptions import ServerTimeoutError
from proxystore.connectors.dim.rpc import RPC
from proxystore.connectors.dim.rpc import RPCResponse
from proxystore.connectors.dim.zmq import wait_for_server
from proxystore.connectors.dim.zmq import ZeroMQConnector
from proxystore.connectors.dim.zmq import ZeroMQServer
from proxystore.serialize import serialize
from testing.compat import randbytes
from testing.utils import open_port

TIMEOUT = 0.2


def test_wait_for_server() -> None:
    with pytest.raises(ServerTimeoutError, match='timeout'):
        wait_for_server('127.0.0.1', open_port(), timeout=0.01)


def test_large_message_sizes() -> None:
    chunk_length = 1000
    with ZeroMQConnector(
        '127.0.0.1',
        open_port(),
        chunk_length=1000,
        timeout=TIMEOUT,
    ) as connector:
        data = randbytes(3 * chunk_length)
        key = connector.put(data)
        assert connector.get(key) is not None
        connector.evict(key)


def test_multiple_connectors() -> None:
    port = open_port()
    # C1 creates the server
    c1 = ZeroMQConnector('127.0.0.1', port, timeout=TIMEOUT)
    c2 = ZeroMQConnector('127.0.0.1', port, timeout=TIMEOUT)

    key = c1.put(b'data')
    assert c2.get(key) == b'data'

    # C2 did not create the server so closing should not kill it
    c2.close()
    assert c1.get(key) == b'data'

    # C1 will actually stop the server
    c1.close()

    with pytest.raises(ServerTimeoutError):
        ZeroMQConnector('127.0.0.1', port, timeout=0.01)


def test_server_errors() -> None:
    server = ZeroMQServer()

    rpc = RPC('exists', key='key', size=1)
    with mock.patch.object(server, 'exists', side_effect=RuntimeError('xyz')):
        response = server.handle_rpc(rpc)
        assert response.exception is not None
        assert 'xyz' in str(response.exception)


def test_handle_server_error_responses(zmq_connector: ZeroMQConnector) -> None:
    connector = ZeroMQConnector.from_config(zmq_connector.config())

    rpc = RPC('exists', key='key', size=1)
    response = RPCResponse(
        'exists',
        key='key',
        size=1,
        exception=RuntimeError('xyz'),
    )
    with mock.patch.object(
        connector.socket,
        'send_multipart',
    ), mock.patch.object(
        connector.socket,
        'recv_multipart',
        return_value=[serialize(response)],
    ):
        with pytest.raises(RuntimeError, match='xyz'):
            connector._send_rpcs([rpc])
