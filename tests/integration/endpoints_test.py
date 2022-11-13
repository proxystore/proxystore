from __future__ import annotations

import asyncio
import os
import pathlib
import uuid
from multiprocessing import Process
from multiprocessing import Queue
from typing import Any
from typing import Generator
from unittest import mock

import pytest

from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import write_config
from proxystore.p2p.client import connect
from proxystore.p2p.server import serve
from proxystore.proxy import Proxy
from proxystore.store import get_store
from proxystore.store.endpoint import EndpointStore
from testing.endpoint import serve_endpoint_silent
from testing.endpoint import wait_for_endpoint
from testing.utils import open_port


async def wait_for_server(host: str, port: int) -> None:
    """Wait for websocket server to be available for connections."""
    while True:
        try:
            _, _, connection = await connect(f'ws://{host}:{port}')
        except OSError:  # pragma: no cover
            await asyncio.sleep(0.01)
        else:
            await connection.close()
            break


def serve_signaling_server(host: str, port: int) -> None:
    """Run signaling server."""
    asyncio.run(serve(host, port))


@pytest.fixture
def endpoints(
    signaling_server,
    tmp_path: pathlib.Path,
) -> Generator[tuple[list[uuid.UUID], list[str]], None, None]:
    """Launch the signaling server and two endpoints."""
    ss_host = 'localhost'
    ss_port = open_port()

    ss = Process(
        target=serve_signaling_server,
        kwargs={'host': ss_host, 'port': ss_port},
    )
    ss.start()

    asyncio.run(wait_for_server(ss_host, ss_port))

    handles = []
    uuids = []
    dirs = []
    for port in (open_port(), open_port()):
        cfg = EndpointConfig(
            name=f'test-endpoint-{port}',
            uuid=uuid.uuid4(),
            host='localhost',
            port=port,
            server=f'ws://{ss_host}:{ss_port}',
        )
        assert cfg.host is not None

        # We want a unique proxystore_dir for each endpoint to simulate
        # different systems
        proxystore_dir = os.path.join(tmp_path, str(port))
        endpoint_dir = os.path.join(proxystore_dir, cfg.name)
        write_config(cfg, endpoint_dir)
        uuids.append(cfg.uuid)
        dirs.append(proxystore_dir)

        handle = Process(target=serve_endpoint_silent, args=[cfg])
        handle.start()
        handles.append(handle)

        wait_for_endpoint(cfg.host, cfg.port)

    if not ss.is_alive():  # pragma: no cover
        raise RuntimeError('Signaling server died.')

    yield uuids, dirs

    for handle in handles:
        handle.terminate()
        handle.join()

    ss.terminate()
    ss.join()


@pytest.mark.integration
def test_endpoint_transfer(endpoints) -> None:
    """Test transferring data between two endpoints."""
    endpoints, proxystore_dirs = endpoints

    store1 = EndpointStore(
        'store1',
        endpoints=endpoints,
        proxystore_dir=proxystore_dirs[0],
    )
    store2 = EndpointStore(
        'store2',
        endpoints=endpoints,
        proxystore_dir=proxystore_dirs[1],
    )

    obj = [1, 2, 3]
    key = store1.set(obj)
    assert obj == store2.get(key)
    store2.evict(key)
    assert not store1.exists(key)


def _produce_local(
    queue: Queue[Any],
    endpoints: list[uuid.UUID],
    home_dir: str,
) -> None:
    store = EndpointStore(
        'store',
        endpoints=endpoints,
        proxystore_dir=home_dir,
    )
    obj = [1, 2, 3]
    proxy: Proxy[Any] = store.proxy(obj)
    queue.put(proxy)


def _consume_local(queue: Queue[Any]) -> None:
    # access proxy to force resolve which will reconstruct store
    obj = queue.get()
    assert obj == [1, 2, 3]


@pytest.mark.integration
def test_endpoint_proxy_transfer(endpoints) -> None:
    """Test transferring data via proxy between processes sharing endpoint."""
    endpoints, proxystore_dirs = endpoints

    queue: Queue[Any] = Queue()

    producer = Process(
        target=_produce_local,
        args=(queue, endpoints, proxystore_dirs[0]),
    )
    consumer = Process(target=_consume_local, args=(queue,))

    producer.start()
    consumer.start()

    producer.join()
    consumer.join()

    if producer.exitcode != 0:  # pragma: no cover
        raise Exception('Caught exception in produce().')

    if consumer.exitcode != 0:  # pragma: no cover
        raise Exception('Caught exception in consume().')


def _produce_remote(
    queue: Queue[Any],
    endpoints: list[uuid.UUID],
    home_dir: str,
) -> None:
    # Mock home_dir to simulate different systems
    with mock.patch(
        'proxystore.store.endpoint.home_dir',
        return_value=home_dir,
    ):
        store = EndpointStore('store', endpoints=endpoints)
        # Send port to other process to compare
        proxy: Proxy[Any] = store.proxy(store.endpoint_port)
        queue.put(proxy)


def _consume_remote(queue: Queue[Any], home_dir: str) -> None:
    # Mock home_dir to simulate different systems
    with mock.patch(
        'proxystore.store.endpoint.home_dir',
        return_value=home_dir,
    ):
        port = queue.get()
        # Just to force the proxy to resolve
        assert isinstance(port, int)

        # Make sure consumer is using different port
        store = get_store('store')
        assert isinstance(store, EndpointStore)
        assert store.endpoint_port != port


@pytest.mark.integration
def test_proxy_detects_endpoint(endpoints) -> None:
    """Test transferring data via proxy between two process and endpoints."""
    endpoints, proxystore_dirs = endpoints

    queue: Queue[Any] = Queue()

    producer = Process(
        target=_produce_remote,
        args=(queue, endpoints, proxystore_dirs[0]),
    )
    consumer = Process(
        target=_consume_remote,
        args=(queue, proxystore_dirs[1]),
    )

    producer.start()
    consumer.start()

    producer.join()
    consumer.join()

    if producer.exitcode != 0:  # pragma: no cover
        raise Exception('Caught exception in produce().')

    if consumer.exitcode != 0:  # pragma: no cover
        raise Exception('Caught exception in consume().')
