from __future__ import annotations

from typing import NamedTuple

import pytest

from proxystore.store.config import ConnectorConfig
from proxystore.store.config import StoreConfig
from proxystore.stream.events import bytes_to_event
from proxystore.stream.events import EndOfStreamEvent
from proxystore.stream.events import Event
from proxystore.stream.events import event_to_bytes
from proxystore.stream.events import EventBatch
from proxystore.stream.events import NewObjectEvent
from proxystore.stream.events import NewObjectKeyEvent


class _TestKey(NamedTuple):
    field1: str
    field2: int


MOCK_CONFIG = StoreConfig(name='test', connector=ConnectorConfig(kind='test'))
MOCK_END_OF_STREAM = EndOfStreamEvent('topic')
MOCK_NEW_OBJECT = NewObjectEvent('topic', 123, {})
MOCK_NEW_OBJECT_KEY = NewObjectKeyEvent.from_key(
    _TestKey('a', 123),
    evict=True,
    metadata={},
    store_config=MOCK_CONFIG,
    topic='topic',
)


@pytest.mark.parametrize(
    'event',
    (
        MOCK_END_OF_STREAM,
        MOCK_NEW_OBJECT,
        MOCK_NEW_OBJECT_KEY,
        EventBatch(
            topic='topic',
            events=[MOCK_NEW_OBJECT, MOCK_NEW_OBJECT_KEY, MOCK_END_OF_STREAM],
        ),
    ),
)
def test_encode_decode(event: Event) -> None:
    json_string = event_to_bytes(event)
    new_event = bytes_to_event(json_string)
    assert event == new_event


def test_new_object_key() -> None:
    key = _TestKey('a', 123)
    event = NewObjectKeyEvent.from_key(
        key,
        evict=True,
        metadata={},
        store_config=MOCK_CONFIG,
        topic='topic',
    )
    new_key = event.get_key()
    assert key == new_key
    assert type(key) is type(new_key)
