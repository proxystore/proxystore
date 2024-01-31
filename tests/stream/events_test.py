from __future__ import annotations

from typing import NamedTuple

import pytest

from proxystore.stream.events import EndOfStreamEvent
from proxystore.stream.events import Event
from proxystore.stream.events import event_to_json
from proxystore.stream.events import json_to_event
from proxystore.stream.events import NewObjectEvent


class _TestKey(NamedTuple):
    field1: str
    field2: int


@pytest.mark.parametrize(
    'event',
    (
        EndOfStreamEvent(),
        NewObjectEvent.from_key(_TestKey('a', 123)),
    ),
)
def test_encode_decode(event: Event) -> None:
    json_string = event_to_json(event)
    new_event = json_to_event(json_string)
    assert event == new_event


def test_new_object_to_from_key() -> None:
    key = _TestKey('a', 123)
    event = NewObjectEvent.from_key(key)
    new_key = event.get_key()
    assert key == new_key
    assert type(key) == type(new_key)
