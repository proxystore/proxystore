"""ZMQConnector Unit Tests."""
from __future__ import annotations

import platform
from unittest import mock

import pytest

from proxystore.connectors.dim.exceptions import ServerTimeoutError
from proxystore.connectors.dim.models import DIMKey
from proxystore.connectors.dim.models import RPC
from proxystore.connectors.dim.models import RPCResponse
from proxystore.connectors.dim.zmq import wait_for_server
from proxystore.connectors.dim.zmq import ZeroMQConnector
from proxystore.connectors.dim.zmq import ZeroMQServer
from proxystore.serialize import serialize
from testing.compat import randbytes
from testing.utils import open_port

if platform.system() == 'Darwin':  # pragma: no cover
    # MacOS GitHub Actions runners are slow
    TIMEOUT = 1.0
else:  # pragma: no cover
    TIMEOUT = 0.5

TEST_KEY = DIMKey(
    'zmq',
    obj_id='key',
    size=0,
    peer_host='localhost',
    peer_port=0,
)


def test_wait_for_server() -> None:
    with pytest.raises(ServerTimeoutError, match='timeout'):
        wait_for_server('127.0.0.1', open_port(), timeout=0.01)


def test_large_message_sizes() -> None:
    chunk_length = 1000
    with ZeroMQConnector(
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
    c1 = ZeroMQConnector(port, timeout=TIMEOUT)
    c2 = ZeroMQConnector(port, timeout=TIMEOUT)

    key = c1.put(b'data')
    assert c2.get(key) == b'data'

    # C2 did not create the server so closing should not kill it
    c2.close()
    assert c1.get(key) == b'data'

    # C1 will actually stop the server
    c1.close()


def test_server_errors() -> None:
    server = ZeroMQServer()

    rpc = RPC('exists', key=TEST_KEY)
    with mock.patch.object(server, 'exists', side_effect=RuntimeError('xyz')):
        response = server.handle_rpc(rpc)
        assert response.exception is not None
        assert 'xyz' in str(response.exception)


def test_handle_server_error_responses() -> None:
    rpc = RPC('exists', TEST_KEY)
    response = RPCResponse(
        'exists',
        key=TEST_KEY,
        exception=RuntimeError('xyz'),
    )

    port = open_port()
    with mock.patch('proxystore.connectors.dim.zmq.wait_for_server'):
        with ZeroMQConnector(port, timeout=TIMEOUT) as connector:
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


def test_provide_ip() -> None:
    host = '127.0.0.1'
    with mock.patch(
        'proxystore.connectors.dim.zmq.wait_for_server',
    ):
        with ZeroMQConnector(port=0, address=host) as connector:
            assert connector.address == host


@pytest.mark.skipif(
    platform.system() == 'Darwin',
    reason=(
        'Resolving an IP address from an interface is not supported on MacOS'
    ),
)
def test_provide_interface() -> None:  # pragma: darwin no cover
    with mock.patch(
        'proxystore.connectors.dim.zmq.wait_for_server',
    ):
        with ZeroMQConnector(port=0, interface='lo') as connector:
            assert connector.address == '127.0.0.1'
