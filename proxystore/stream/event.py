"""Event metadata type."""
from __future__ import annotations

import dataclasses
import json
from typing import Any
from typing import Generic
from typing import NamedTuple
from typing import TypeVar

from proxystore.utils.imports import get_class_path
from proxystore.utils.imports import import_class

KeyT = TypeVar('KeyT', bound=NamedTuple)


@dataclasses.dataclass
class Event(Generic[KeyT]):
    """Event metadata for a stream.

    Warning:
        This is not considered a public API and may change at any time
        without warning. [`Event`][proxystore.stream.event.Event] instances
        are created and consumed internally by the
        [StreamProducer][proxystore.stream.interface.StreamProducer] and
        [StreamConsumer][proxystore.stream.interface.StreamConsumer] and
        never exposed to client code.
    """

    key_type: str
    raw_key: tuple[Any, ...]
    evict: bool

    @classmethod
    def from_key(cls, key: KeyT, *, evict: bool = True) -> Event[KeyT]:
        """Create a new event from a key to a stored object."""
        return Event(
            key_type=get_class_path(type(key)),
            raw_key=tuple(key),
            evict=evict,
        )

    @classmethod
    def from_json(cls, payload: str) -> Event[KeyT]:
        """Create a new event from its JSON representation."""
        return Event(**json.loads(payload))

    def as_json(self) -> str:
        """Convert the event to a JSON string."""
        return json.dumps(dataclasses.asdict(self))

    def get_key(self) -> KeyT:
        """Get the object key associated with the event."""
        key_type = import_class(self.key_type)
        return key_type(*self.raw_key)
