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

    pass


@dataclasses.dataclass
class NewObjectEvent:
    """New object in stream event metadata."""

    key_type: str
    raw_key: list[Any]
    evict: bool
    topic: str
    store_config: dict[str, Any]

    @classmethod
    def from_key(
        cls,
        key: Any,
        topic: str,
        store_config: dict[str, Any],
        *,
        evict: bool = True,
    ) -> NewObjectEvent:
        """Create a new event from a key to a stored object."""
        return NewObjectEvent(
            key_type=get_class_path(type(key)),
            raw_key=list(key),
            evict=evict,
            topic=topic,
            store_config=store_config,
        )

    def get_key(self) -> Any:
        """Get the object key associated with the event."""
        key_type = import_class(self.key_type)
        return key_type(*self.raw_key)


class _EventMapping(enum.Enum):
    END_OF_STREAM = EndOfStreamEvent
    NEW_OBJECT = NewObjectEvent


Event = Union[EndOfStreamEvent, NewObjectEvent]
"""Event union type."""


def event_to_json(event: Event) -> str:
    """Convert event to JSON string."""
    data = dataclasses.asdict(event)
    data['event_type'] = _EventMapping(type(event)).name
    return json.dumps(data)


def json_to_event(s: str) -> Event:
    """Convert JSON string to event."""
    data = json.loads(s)
    event_type = data.pop('event_type')
    event = _EventMapping[event_type].value(**data)
    return event
