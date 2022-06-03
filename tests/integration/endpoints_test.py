from __future__ import annotations

import os
import uuid
from multiprocessing import Process
from multiprocessing import Queue
from typing import Generator
from unittest import mock

import pytest

from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import write_config
from proxystore.p2p.server import serve
from proxystore.store import get_store
from proxystore.store.endpoint import EndpointStore
from testing.endpoint import launch_endpoint


@pytest.fixture
def endpoints(tmp_dir) -> Generator[tuple[list[str], list[str]], None, None]:
    """Launch the signaling server and two endpoints."""
    ss_host = 'localhost'
    ss_port = 5123

    def serve_signaling_server() -> None:
        import asyncio

        asyncio.run(serve(ss_host, ss_port))

    ss = Process(target=serve_signaling_server)
    ss.start()

    handles = []
    uuids = []
    dirs = []
    for port in (5100, 5101):
        cfg = EndpointConfig(
            name=f'test-endpoint-{port}',
            uuid=str(uuid.uuid4()),
            host='localhost',
            port=port,
            server=f'{ss_host}:{ss_port}',
        )
        # We want a unique proxystore_dir for each endpoint to simulate
        # different systems
        proxystore_dir = os.path.join(tmp_dir, str(port))
        endpoint_dir = os.path.join(proxystore_dir, cfg.name)
        write_config(cfg, endpoint_dir)
        uuids.append(cfg.uuid)
        dirs.append(proxystore_dir)
        handles.append(
            launch_endpoint(
                cfg.name,
                cfg.uuid,
                cfg.host,
                cfg.port,
                cfg.server,
            ),
        )

    yield uuids, dirs

    for handle in handles:
        handle.terminate()
    ss.terminate()


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


def test_endpoint_proxy_transfer(endpoints) -> None:
    """Test transferring data via proxy between processes sharing endpoint."""
    endpoints, proxystore_dirs = endpoints

    def produce(queue: Queue) -> None:
        store = EndpointStore(
            'store',
            endpoints=endpoints,
            proxystore_dir=proxystore_dirs[0],
        )
        obj = [1, 2, 3]
        proxy = store.proxy(obj)
        queue.put(proxy)

    def consume(queue: Queue) -> None:
        # access proxy to force resolve which will reconstruct store
        obj = queue.get()
        assert obj == [1, 2, 3]

    queue = Queue()

    producer = Process(target=produce, args=(queue,))
    consumer = Process(target=consume, args=(queue,))

    producer.start()
    consumer.start()

    producer.join()
    consumer.join()

    if producer.exitcode != 0:  # pragma: no cover
        raise Exception('Caught exception in produce().')

    if consumer.exitcode != 0:  # pragma: no cover
        raise Exception('Caught exception in consume().')


def test_proxy_detects_endpoint(endpoints) -> None:
    """Test transferring data via proxy between two process and endpoints."""
    endpoints, proxystore_dirs = endpoints

    # Mock default_dir to simulate different systems
    def produce(queue: Queue) -> None:
        with mock.patch(
            'proxystore.store.endpoint.default_dir',
            return_value=proxystore_dirs[0],
        ):
            store = EndpointStore('store', endpoints=endpoints)
            obj = [1, 2, 3]
            proxy = store.proxy(obj)
            queue.put(proxy)

    def consume(queue: Queue) -> None:
        with mock.patch(
            'proxystore.store.endpoint.default_dir',
            return_value=proxystore_dirs[1],
        ):
            obj = queue.get()
            # Access proxy to force it to resolve which will in turn create
            # a new store instance
            assert obj == [1, 2, 3]

            # Make sure consumer is using second endpoint on port 5101
            store = get_store('store')
            assert store.endpoint_port == 5101

    queue = Queue()

    producer = Process(target=produce, args=(queue,))
    consumer = Process(target=consume, args=(queue,))

    producer.start()
    consumer.start()

    producer.join()
    consumer.join()

    if producer.exitcode != 0:  # pragma: no cover
        raise Exception('Caught exception in produce().')

    if consumer.exitcode != 0:  # pragma: no cover
        raise Exception('Caught exception in consume().')
