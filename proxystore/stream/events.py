"""Event metadata type.

Warning:
    Event types are not considered as part of the public API and may change
    at any time without warning. [`Events`][proxystore.stream.events.Event]
    are created and consumed internally by the
    [StreamProducer][proxystore.stream.interface.StreamProducer] and
    [StreamConsumer][proxystore.stream.interface.StreamConsumer] and
    never exposed to client code.
"""
from __future__ import annotations

import dataclasses
import enum
import json
from typing import Any
from typing import Union

from proxystore.utils.imports import get_class_path
from proxystore.utils.imports import import_class


@dataclasses.dataclass
class EndOfStreamEvent:
    """End of stream event."""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EndOfStreamEvent:
        """Create a new event instance from its dictionary representation."""
        return cls()


@dataclasses.dataclass
class NewObjectEvent:
    """New object in stream event metadata."""

    key_type: str
    raw_key: list[Any]
    evict: bool
    metadata: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> NewObjectEvent:
        """Create a new event instance from its dictionary representation."""
        return NewObjectEvent(**data)

    @classmethod
    def from_key(
        cls,
        key: tuple[Any, ...],
        evict: bool,
        metadata: dict[str, Any],
    ) -> NewObjectEvent:
        """Create a new event from a key and metadata."""
        return cls(
            key_type=get_class_path(type(key)),
            raw_key=list(key),
            evict=evict,
            metadata=metadata,
        )

    def get_key(self) -> Any:
        """Get the object key associated with the event."""
        key_type = import_class(self.key_type)
        return key_type(*self.raw_key)


Event = Union[EndOfStreamEvent, NewObjectEvent]
"""Event union type."""


@dataclasses.dataclass
class EventBatch:
    """Batch of stream events."""

    events: list[Event]
    topic: str
    store_config: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EventBatch:
        """Create a new event instance from its dictionary representation."""
        events = [dict_to_event(d) for d in data['events']]
        return cls(
            events=events,  # type: ignore[arg-type]
            topic=data['topic'],
            store_config=data['store_config'],
        )


class _EventMapping(enum.Enum):
    END_OF_STREAM = EndOfStreamEvent
    EVENT_BATCH = EventBatch
    NEW_OBJECT = NewObjectEvent


def event_to_dict(event: Event | EventBatch) -> dict[str, Any]:
    """Convert event to dict."""
    if isinstance(event, EventBatch):
        data = {
            'events': [event_to_dict(e) for e in event.events],
            'topic': event.topic,
            'store_config': event.store_config,
        }
    else:
        data = dataclasses.asdict(event)
    data['event_type'] = _EventMapping(type(event)).name
    return data


def dict_to_event(data: dict[str, Any]) -> Event | EventBatch:
    """Convert dict to event."""
    event_type = data.pop('event_type')
    event = _EventMapping[event_type].value.from_dict(data)
    return event


def event_to_bytes(event: Event | EventBatch) -> bytes:
    """Convert event to byte-string."""
    data = event_to_dict(event)
    return json.dumps(data).encode()


def bytes_to_event(s: bytes) -> Event | EventBatch:
    """Convert byte-string to event."""
    data = json.loads(s.decode())
    return dict_to_event(data)
