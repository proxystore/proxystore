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
        self._next_uuid: str | None = self.id
        self.key: KeyT | None = self.append(None)

    def append(self, obj: Any) -> KeyT:
        # store object and get key reference to object
        key = self.store.put(obj, key_id=self._next_uuid)
        self._next_uuid = key.next_id
        return key

    def close_stream(self) -> None:
        self.store.put(None, id=None)
        self._next_uuid = None


class ProxyStreamConsumer:
    def __init__(self, store: Store[ConnectorT], stream: KeyT | None):
        self.store = store
        self.stream_key = stream

    def __iter__(self):
        while self.stream_key is not None:
            try:
                self.stream_key, obj = self.store.get(self.stream_key)
                obj = deserialize(
                    obj,
                )  # TODO: deserialize using the store's serializer

                if (
                    obj is None
                ):  # keep looping if data hasn't been produced yet
                    continue

                yield obj
            except TypeError:
                return None
