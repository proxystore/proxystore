from __future__ import annotations

import pathlib
import pickle

import pytest

import proxystore.store
from proxystore.connectors.file import FileConnector
from proxystore.proxy import Proxy
from proxystore.store import get_or_create_store
from proxystore.store import get_store
from proxystore.store.base import Store
from proxystore.store.lifetimes import ContextLifetime
from proxystore.stream import StreamConsumer
from proxystore.stream import StreamProducer
from testing.stream import create_message_pubsub_pair


def test_owner_store_clears_connector(tmp_path: pathlib.Path) -> None:
    store = Store(FileConnector(str(tmp_path / 'store')))
    assert store.owner
    store.close()
    assert not (tmp_path / 'store').exists()


def test_owner_store_clear_override(tmp_path: pathlib.Path) -> None:
    store = Store(FileConnector(str(tmp_path / 'store')))
    store.close(clear=False)
    assert (tmp_path / 'store').exists()


def test_non_owner_store_does_not_clear(tmp_path: pathlib.Path) -> None:
    store = Store(FileConnector(str(tmp_path / 'store')), owner=False)
    assert not store.owner
    store.close()
    assert (tmp_path / 'store').exists()


def test_non_owner_store_clear_override(tmp_path: pathlib.Path) -> None:
    store = Store(FileConnector(str(tmp_path / 'store')), owner=False)
    store.close(clear=True)
    assert not (tmp_path / 'store').exists()


@pytest.mark.parametrize('owner', (True, False))
def test_from_config_owner(owner: bool, tmp_path: pathlib.Path) -> None:
    with Store(FileConnector(str(tmp_path / 'store'))) as store:
        new_store = Store.from_config(store.config(), owner=owner)
        assert new_store.owner == owner
        new_store.close()
        assert (tmp_path / 'store').exists() != owner


def test_get_or_create_store_not_owner(tmp_path: pathlib.Path) -> None:
    with Store(FileConnector(str(tmp_path / 'store'))) as store:
        config = store.config()
        proxystore.store._unregister_store(store)

        new_store = get_or_create_store(config)
        assert new_store is not store
        assert not new_store.owner
        new_store.close()
        assert (tmp_path / 'store').exists()


def test_resolved_proxy_store_does_not_clear(tmp_path: pathlib.Path) -> None:
    with Store(FileConnector(str(tmp_path / 'store'))) as store:
        proxy = store.proxy('value', populate_target=False)
        # Simulate resolving the proxy in a different process.
        proxystore.store._unregister_store(store)
        proxy = pickle.loads(pickle.dumps(proxy))
        assert proxy == 'value'

        implicit_store = get_store(proxy)
        assert implicit_store is not store
        assert not implicit_store.owner

        # Closing the implicit store via a lifetime should not delete the
        # data of the owner store.
        lifetime = ContextLifetime(implicit_store)
        lifetime.close(close_stores=True)
        assert (tmp_path / 'store').exists()
        assert store.exists(store.put('other'))


def test_stream_consumer_close_stores_does_not_clear(
    tmp_path: pathlib.Path,
) -> None:
    topic = 'default'
    publisher, subscriber = create_message_pubsub_pair(topic)

    with Store(FileConnector(str(tmp_path / 'store'))) as store:
        producer = StreamProducer[str](publisher, default_store=store)
        consumer = StreamConsumer[str](subscriber)

        producer.send(topic, 'value', evict=False)
        proxystore.store._unregister_store(store)
        item: Proxy[str] = consumer.next()  # type: ignore[assignment]
        assert item == 'value'

        consumer.close(stores=True)
        assert (tmp_path / 'store').exists()
        producer.close()
