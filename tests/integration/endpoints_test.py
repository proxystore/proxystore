from __future__ import annotations

import os
import shutil
import uuid
from multiprocessing import Process
from multiprocessing import Queue
from typing import Generator
from unittest import mock

import pytest

from proxystore.endpoint.config import get_config
from proxystore.p2p.server import serve
from proxystore.store import get_store
from proxystore.store.endpoint import EndpointStore
from testing.endpoint import launch_endpoint


@pytest.fixture
def endpoints() -> Generator[list[tuple[str, int]], None, None]:
    """Launch the signaling server and two endpoints."""
    host = 'localhost'
    ports = (5100, 5101)

    def serve_signaling_server() -> None:
        import asyncio

        asyncio.run(serve('localhost', 5123))

    ss = Process(target=serve_signaling_server)
    ss.start()

    handles = []
    dirs = []
    for port in ports:
        d = f'/tmp/{uuid.uuid4()}'
        handles.append(launch_endpoint(host, port, d, 'localhost:5123'))
        dirs.append(d)

    yield dirs

    for handle in handles:
        handle.terminate()
    ss.terminate()
    for d in dirs:
        if os.path.exists(d):  # pragma: no branch
            shutil.rmtree(d)


def test_endpoint_transfer(endpoints) -> None:
    """Test transferring data between two endpoints."""
    store1 = EndpointStore('store1', endpoint_dir=endpoints[0])
    store2 = EndpointStore('store2', endpoint_dir=endpoints[1])

    obj = [1, 2, 3]
    key = store1.set(obj)
    assert obj == store2.get(key)
    store2.evict(key)
    assert not store1.exists(key)


def test_endpoint_proxy_transfer(endpoints) -> None:
    """Test transferring data via proxy between two process/endpoints."""

    def produce(queue: Queue) -> None:
        store = EndpointStore('store', endpoint_dir=endpoints[0])
        obj = [1, 2, 3]
        proxy = store.proxy(obj)
        queue.put(proxy)

    def consume(queue: Queue) -> None:
        # proxy will reconstruct store using endpoint_dir=endpoints[0]
        obj = queue.get()
        assert obj == [1, 2, 3]

    queue = Queue()

    producer = Process(target=produce, args=(queue,))
    consumer = Process(target=consume, args=(queue,))

    producer.start()
    consumer.start()

    producer.join()
    consumer.join()


def test_proxy_detects_endpoint(endpoints) -> None:
    """Test transferring data via proxy between two process/endpoints.

    Do not specify endpoint to make sure proxies can find correct ones.
    """

    def produce(queue: Queue) -> None:
        with mock.patch(
            'proxystore.endpoint.config.default_dir',
            return_value=endpoints[0],
        ):
            store = EndpointStore('store')
            obj = [1, 2, 3]
            proxy = store.proxy(obj)
            queue.put(proxy)

    def consume(queue: Queue) -> None:
        with mock.patch(
            'proxystore.endpoint.config.default_dir',
            return_value=endpoints[1],
        ):
            obj = queue.get()
            # Access proxy to force it to resolve which will in turn create
            # a new store instance
            assert obj == [1, 2, 3]

            # Get the newly created store and make sure it loaded params
            # from correct config file
            store = get_store('store')
            cfg = get_config(endpoints[1])
            assert store.hostname == cfg.host
            assert store.port == cfg.port

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
