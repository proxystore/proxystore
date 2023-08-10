"""ProxyStream implementation."""
from __future__ import annotations

import logging
import uuid
from typing import Any
from typing import NamedTuple
from typing import TypeVar

from proxystore.connectors.connector import Connector
from proxystore.proxy import Proxy
from proxystore.serialize import deserialize
from proxystore.store import Store
from proxystore.store import get_store

ConnectorT = TypeVar('ConnectorT', bound=Connector[Any])

KeyT = TypeVar('KeyT', bound=NamedTuple)

logger = logging.getLogger(__name__)


class ProxyStreamProducer:
    def __init__(self, store: Store[ConnectorT]):
        self.store = store
        self.id = str(uuid.uuid4())
        self._next_uuid: str | None = self.id
        self._key: KeyT = self.append(None)
        self.proxy = self.store.proxy_from_key(self._key)

    def append(self, obj: Any) -> KeyT:
        # store object and get key reference to object
        key = self.store.put(obj, key_id=self._next_uuid)
        self._next_uuid = key.next_id
        return key

    def close_stream(self) -> None:
        self.store.put(None, key_id=None)
        self._next_uuid = None


class ProxyStreamConsumer:
    def __init__(self, stream: Proxy):
        self.store = get_store(stream)
        self.stream_proxy = stream
        self.stream_key = None

    def __iter__(self):
        (
            self.stream_key,
            obj,
        ) = (
            self.stream_proxy
        )  # resolve the 'head' proxy which doesn't contain data

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
