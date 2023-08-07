"""ProxyStream implementation."""
from __future__ import annotations

import uuid
from typing import Any
from typing import NamedTuple
from typing import TypeVar

from proxystore.connectors.connector import Connector
from proxystore.serialize import deserialize
from proxystore.store import Store

ConnectorT = TypeVar('ConnectorT', bound=Connector[Any])

KeyT = TypeVar('KeyT', bound=NamedTuple)


class ProxyStreamProducer:
    def __init__(self, store: Store[ConnectorT]):
        self.store = store
        self.id = str(uuid.uuid4())
        self._next_uuid = self.id

    def append(self, obj) -> KeyT:
        # store object and get key reference to object
        key = self.store.put(obj, id=self._next_uuid)
        self._next_uuid = key.next_id
        return key

    def close_stream(self) -> None:
        self.store.put(None, id=None)
        self._next_uuid = None


class ProxyStreamConsumer:
    def __init__(self, store: Store[ConnectorT], stream: KeyT):
        self.store = store
        self.stream_key = stream

    def __iter__(self):
        while self.stream_key is not None:
            try:
                self.stream_key, obj = self.store.get(self.stream_key)

                if (
                    obj is None
                ):  # keep looping if data hasn't been produced yet
                    continue

                yield deserialize(obj)
            except TypeError:
                return None


class ProxyStream:
    def __init__(self):
        self.proxy_uuids = []
        self.is_end = False
        self.connected_clients: dict[str, int] = {}

    def append(self, data):
        self.proxy_uuids.append(data)

    def next_data(self, host):
        try:
            data = self.proxy_uuids[self.connected_clients[host]]
            self.connected_clients[host] += 1
            return data
        except IndexError:
            return None

    def is_end_of_stream(self):
        return self.is_end

    def end_stream(self):
        self.is_end = True

    def connect(self, host):
        if host not in self.connected_clients:
            self.connected_clients[host] = len(self.proxy_uuids) - 1
