from __future__ import annotations

import atexit
import contextlib
import json
import os
import socket
import subprocess
import sys
import threading
import warnings
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

import pytest
import zmq

from proxystore.connectors.zmq import _kill_server
from proxystore.connectors.zmq import _reset_pools_after_fork
from proxystore.connectors.zmq import _SocketPool
from proxystore.connectors.zmq import get_interface_address
from proxystore.connectors.zmq import run_server
from proxystore.connectors.zmq import ServerTimeoutError
from proxystore.connectors.zmq import spawn_server
from proxystore.connectors.zmq import wait_for_server
from proxystore.connectors.zmq import ZeroMQConnector
from proxystore.connectors.zmq import ZeroMQKey
from proxystore.connectors.zmq import ZeroMQServer
from proxystore.connectors.zmq import ZeroMQServerError
from testing.compat import randbytes
from testing.utils import open_port

ADDRESS = '127.0.0.1'


@contextlib.contextmanager
def thread_server(port: int) -> Generator[None, None, None]:
    """Run a server in a thread of this process so it is covered."""
    stop = threading.Event()
    thread = threading.Thread(
        target=run_server,
        args=(ADDRESS, port),
        kwargs={'stop': stop, 'poll_interval': 0.01},
    )
    thread.start()
    try:
        wait_for_server(ADDRESS, port)
        yield
    finally:
        stop.set()
        thread.join()


@pytest.fixture
def server_port() -> Generator[int, None, None]:
    port = open_port()
    with thread_server(port):
        yield port


@pytest.fixture
def connector(server_port: int) -> Generator[ZeroMQConnector, None, None]:
    with ZeroMQConnector(server_port, address=ADDRESS) as connector:
        assert connector.server is None
        yield connector


def test_basic_ops(connector: ZeroMQConnector) -> None:
    key = connector.put(b'value')
    assert connector.exists(key)
    assert connector.get(key) == b'value'
    connector.evict(key)
    assert not connector.exists(key)
    assert connector.get(key) is None

    key = connector.new_key()
    connector.set(key, b'value')
    assert connector.get(key) == b'value'


def test_large_objects(connector: ZeroMQConnector) -> None:
    data = randbytes(10 * 1000 * 1000)
    key = connector.put(data)
    assert connector.get(key) == data
    connector.evict(key)


def test_batch_ops_across_servers(connector: ZeroMQConnector) -> None:
    with ZeroMQConnector(open_port(), address=ADDRESS) as other:
        try:
            keys = connector.put_batch([b'a', b'b'])
            keys += other.put_batch([b'c', b'd'])
            # Interleave keys from both servers
            keys = [keys[0], keys[2], keys[1], keys[3]]
            assert connector.get_batch(keys) == [b'a', b'c', b'b', b'd']
        finally:
            other.close(kill_server=True)


def test_concurrent_threads(connector: ZeroMQConnector) -> None:
    def _put_get(index: int) -> bool:
        data = str(index).encode()
        key = connector.put(data)
        return connector.get(key) == data

    with ThreadPoolExecutor(8) as pool:
        assert all(pool.map(_put_get, range(200)))


def test_server_shared_between_connectors() -> None:
    port = open_port()
    owner = ZeroMQConnector(port, address=ADDRESS, timeout=10)
    assert owner.server is not None
    server = owner.server
    key = owner.put(b'value')

    other = ZeroMQConnector(port, address=ADDRESS)
    assert other.server is None
    # Only the connector which spawned the server can stop it
    other.close(kill_server=True)

    # By default, closing the owner does not stop the server
    owner.close()
    with ZeroMQConnector(port, address=ADDRESS) as connector:
        assert connector.get(key) == b'value'

    _kill_server(server)
    atexit.unregister(owner._kill_hook)


def test_close_kill_server() -> None:
    port = open_port()
    connector = ZeroMQConnector(port, address=ADDRESS, timeout=10)
    connector.close(kill_server=True)
    assert connector.server is None

    with pytest.raises(ServerTimeoutError):
        wait_for_server(ADDRESS, port, timeout=0.1)


def test_server_restart() -> None:
    port = open_port()
    with thread_server(port):
        connector = ZeroMQConnector(port, address=ADDRESS, request_timeout=0.1)
        key = connector.put(b'value')
        assert connector.get(key) == b'value'

    # Requests fail while the server is down
    with pytest.raises(ServerTimeoutError):
        connector.get(key)

    # Reconnecting to the restarted server can take longer than 0.1 seconds
    connector.request_timeout = 5
    with thread_server(port):
        # Objects stored in the previous server are lost
        assert connector.get(key) is None
        key = connector.put(b'value')
        assert connector.get(key) == b'value'

    # Idle sockets in the pool reconnect to the new server
    with thread_server(port):
        with connector._pool._lock:
            idle = list(connector._pool._idle[connector.url])
        assert len(idle) > 0
        assert not connector.exists(key)
        with connector._pool._lock:
            assert connector._pool._idle[connector.url] == idle

    connector.close()


# Each process creates a connector to the same port at the same time then
# reports the key of an object it put, the PID of the server, and if it
# spawned the server. Then it checks it can get the objects put by the other
# processes and waits for stdin to be closed before exiting because the
# server exits with the process that spawned it.
_SPAWN_RACE_SCRIPT = """\
import json
import sys
from proxystore.connectors.zmq import ZeroMQConnector
from proxystore.connectors.zmq import ZeroMQKey
from proxystore.connectors.zmq import wait_for_server

address, port, index = sys.argv[1], int(sys.argv[2]), sys.argv[3]
print('ready', flush=True)
sys.stdin.readline()

connector = ZeroMQConnector(port, address=address, timeout=30)
key = connector.put(index.encode())
pid = wait_for_server(address, port)
print(json.dumps([key, pid, connector.server is not None]), flush=True)

keys = [ZeroMQKey(*key) for key in json.loads(sys.stdin.readline())]
values = [bytes(connector.get(key)).decode() for key in keys]
print(json.dumps(values), flush=True)
sys.stdin.read()
"""


def _read_line(process: subprocess.Popen[str]) -> str:
    assert process.stdout is not None
    return process.stdout.readline().strip()


def _write_line(process: subprocess.Popen[str], line: str) -> None:
    assert process.stdin is not None
    process.stdin.write(f'{line}\n')
    process.stdin.flush()


def test_spawn_race_between_processes() -> None:
    port = open_port()
    processes = [
        subprocess.Popen(
            [
                sys.executable,
                '-c',
                _SPAWN_RACE_SCRIPT,
                ADDRESS,
                str(port),
                str(i),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        for i in range(8)
    ]
    try:
        for process in processes:
            assert _read_line(process) == 'ready'
        # Start all processes at once after they have imported everything
        for process in processes:
            _write_line(process, 'go')

        results = [json.loads(_read_line(p)) for p in processes]
        keys = [key for key, _, _ in results]
        # Exactly one process spawned the server and all use the same server
        assert sum(spawned for _, _, spawned in results) == 1
        assert len({pid for _, pid, _ in results}) == 1

        for process in processes:
            _write_line(process, json.dumps(keys))
        expected = [str(i) for i in range(len(processes))]
        for process in processes:
            assert json.loads(_read_line(process)) == expected
    finally:
        for process in processes:
            assert process.stdin is not None
            process.stdin.close()
        for process in processes:
            assert process.wait(timeout=30) == 0

    # The server exited with the process that spawned it
    with pytest.raises(ServerTimeoutError):
        wait_for_server(ADDRESS, port, timeout=0.1)


def test_request_timeout(server_port: int) -> None:
    with ZeroMQConnector(
        server_port,
        address=ADDRESS,
        request_timeout=0.1,
    ) as connector:
        missing = ZeroMQKey('id', peer_host=ADDRESS, peer_port=open_port())
        with pytest.raises(ServerTimeoutError, match='did not respond'):
            connector.get(missing)

        # Requests to other servers still work
        assert not connector.exists(connector.new_key())


def test_server_error_raised_on_client(connector: ZeroMQConnector) -> None:
    header = b'{"op": "unknown", "obj_id": "id"}'
    with (
        mock.patch(
            'proxystore.connectors.zmq._request_header',
            return_value=header,
        ),
        pytest.raises(ZeroMQServerError, match='Unknown operation'),
    ):
        connector.exists(connector.new_key())

    # The connector is still usable after an error
    key = connector.put(b'value')
    assert connector.get(key) == b'value'


@pytest.mark.parametrize(
    ('frames', 'match'),
    (
        ([b'id'], 'Expected 2 or 3 frames'),
        ([b'id', b'{}', b'', b''], 'Expected 2 or 3 frames'),
        ([b'id', b'\xff'], 'UnicodeDecodeError'),
        ([b'id', b'not json'], 'JSONDecodeError'),
        ([b'id', b'[]'], 'must be a JSON object'),
        ([b'id', b'{"op": "get"}'], 'must contain a string obj_id'),
        ([b'id', b'{"op": "put", "obj_id": "x"}'], 'missing the data'),
        ([b'id', b'{"op": "?", "obj_id": "x"}'], 'Unknown operation'),
    ),
)
def test_server_malformed_requests(
    server_port: int,
    frames: list[bytes],
    match: str,
) -> None:
    with zmq.Context() as context, context.socket(zmq.DEALER) as sock:
        sock.setsockopt(zmq.LINGER, 0)
        sock.connect(f'tcp://{ADDRESS}:{server_port}')
        sock.send_multipart(frames)
        assert sock.poll(5000)
        request_id, header = sock.recv_multipart()

    assert request_id == b'id'
    assert match in header.decode()

    # The server is still running
    assert wait_for_server(ADDRESS, server_port) == os.getpid()


def test_server_empty_request() -> None:
    request_id, header = ZeroMQServer().handle([])
    assert request_id == b''
    assert b'Expected 2 or 3 frames' in header


def test_wait_for_server_timeout() -> None:
    with pytest.raises(ServerTimeoutError, match='did not respond'):
        wait_for_server(ADDRESS, open_port(), timeout=0.01)


def test_spawn_server_already_running(server_port: int) -> None:
    # The spawned server fails to bind so the server in this process is used
    assert spawn_server(ADDRESS, server_port, timeout=10) is None


def test_spawn_server_port_in_use() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((ADDRESS, 0))
        sock.listen()
        port = sock.getsockname()[1]
        # The port is open but not used by a server so the connector tries
        # to spawn one which fails to bind.
        with pytest.raises(ServerTimeoutError):
            ZeroMQConnector(port, address=ADDRESS, timeout=0.5)


def test_resolve_address_from_hostname(server_port: int) -> None:
    with (
        mock.patch('socket.gethostbyname', return_value=ADDRESS),
        pytest.warns(UserWarning, match='loopback address'),
    ):
        connector = ZeroMQConnector(server_port)
    assert connector.address == ADDRESS
    assert connector.config()['address'] is None
    connector.close()


def test_resolve_non_loopback_address_from_hostname() -> None:
    address = '192.0.2.1'
    with (
        mock.patch('socket.gethostbyname', return_value=address),
        mock.patch('socket.create_connection'),
        mock.patch('proxystore.connectors.zmq.wait_for_server'),
        warnings.catch_warnings(action='error'),
    ):
        connector = ZeroMQConnector(open_port())
    assert connector.address == address
    connector.close()


@pytest.mark.skipif(
    not sys.platform.startswith('linux'),
    reason='Getting the address of an interface is only supported on Linux.',
)
def test_resolve_address_from_interface(server_port: int) -> None:
    assert get_interface_address('lo') == ADDRESS
    with ZeroMQConnector(server_port, interface='lo') as connector:
        assert connector.address == ADDRESS


def test_socket_pool_after_fork(connector: ZeroMQConnector) -> None:
    assert not connector.exists(connector.new_key())
    parent_context = connector._pool._context
    parent_lock = connector._pool._lock
    parent_sockets = [
        sock for sockets in connector._pool._idle.values() for sock in sockets
    ]

    # Simulate the fork handler running in a forked child process
    _reset_pools_after_fork()
    assert connector._pool._context is not parent_context
    assert connector._pool._lock is not parent_lock
    assert all(len(idle) == 0 for idle in connector._pool._idle.values())
    # This process was not actually forked so close the parent's sockets
    for sock in parent_sockets:
        sock.close()
    parent_context.term()

    assert not connector.exists(connector.new_key())


def test_socket_pool_release_after_close() -> None:
    pool = _SocketPool()
    sock = pool.acquire(f'tcp://{ADDRESS}:{open_port()}')
    pool.close()
    pool.release('url', sock)
    assert sock.closed
