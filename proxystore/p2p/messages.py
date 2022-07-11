"""Message types for peer-to-peer communication."""
from __future__ import annotations

import dataclasses
import enum
import json
import sys
import uuid
from typing import Any

if sys.version_info >= (3, 9):  # pragma: >=3.9 cover
    from typing import Literal
else:  # pragma: <3.9 cover
    from typing_extensions import Literal


class MessageType(enum.Enum):
    """Types of messages supported."""

    server_response = 'ServerResponse'
    server_registration = 'ServerRegistration'
    peer_connection = 'PeerConnection'
    peer_message = 'PeerMessage'


@dataclasses.dataclass
class Message:
    """Base message."""

    pass


@dataclasses.dataclass
class ServerRegistration(Message):
    """Register with signaling server as peer."""

    name: str
    uuid: uuid.UUID
    message_type: str = MessageType.server_registration.name


@dataclasses.dataclass
class ServerResponse(Message):
    """Message returned by signaling server on success or error."""

    success: bool = True
    message: str | None = None
    error: bool = False
    message_type: str = MessageType.server_response.name


@dataclasses.dataclass
class PeerConnection(Message):
    """Message used in establishing a peer-to-peer connection."""

    source_uuid: uuid.UUID
    source_name: str
    peer_uuid: uuid.UUID
    description_type: Literal['answer', 'offer']
    description: str
    error: str | None = None
    message_type: str = MessageType.peer_connection.name


@dataclasses.dataclass
class PeerMessage(Message):
    """Message sent between peers."""

    source_uuid: uuid.UUID
    peer_uuid: uuid.UUID
    message: str
    error: bool = False
    message_type: str = MessageType.peer_message.name


class MessageDecodeError(Exception):
    """Error raised when a message cannot be decoded."""

    pass


class MessageEncodeError(Exception):
    """Error raised when an message cannot be encoded."""

    pass


def uuid_to_str(data: dict[str, Any]) -> dict[str, Any]:
    """Cast any UUIDs to strings.

    Scans the input dictionary for any values where the associated key
    contains 'uuid' and value is a UUID instance and converts it to a
    string for jsonification.

    Returns:
        Shallow copy of the input dictionary with values cast from UUID
        to str if their key also contains UUID.
    """
    data = data.copy()
    for key in data:
        if 'uuid' in key.lower() and isinstance(data[key], uuid.UUID):
            data[key] = str(data[key])
    return data


def str_to_uuid(data: dict[str, Any]) -> dict[str, Any]:
    """Cast any possible UUID strings to UUID objects.

    The inverse operation of :func:`<._uuid_to_str>`.

    Returns:
        Shallow copy of the input dictionary with values cast from
        str to UUID if the key also contains UUID.

    Raises:
        MessageDecodeError:
            if a key contains 'uuid' but the value cannot be cast to a UUID.
    """
    data = data.copy()
    for key in data:
        if 'uuid' in key.lower():
            try:
                data[key] = uuid.UUID(data[key])
            except (AttributeError, TypeError, ValueError) as e:
                raise MessageDecodeError(
                    f'Failed to convert key {key} to UUID: {e}',
                )
    return data


def decode(message: str) -> Message:
    """Decode JSON string into correct Message type.

    Args:
        message (str): JSON string to decode.

    Returns:
        Instance of a subtype of
        :any:`Message <proxystore.p2p.messages.Message>`.

    Raises:
        MessageDecodeError:
            if the message cannot be decoded into a
            :any:`Message <proxystore.p2p.messages.Message>`.
    """
    try:
        data = json.loads(message)
    except json.JSONDecodeError as e:
        raise MessageDecodeError(f'Failed to load string as JSON: {e}')

    try:
        message_type_name = data.pop('message_type')
    except KeyError:
        raise MessageDecodeError(
            'Message does not contain a message_type key.',
        )

    try:
        message_type = getattr(
            sys.modules[__name__],
            MessageType[message_type_name].value,
        )
    except (AttributeError, KeyError):
        raise MessageDecodeError(
            'The message is of an unknown message type: '
            f'{message_type_name}.',
        )

    data = str_to_uuid(data)

    try:
        return message_type(**data)
    except TypeError as e:
        raise MessageDecodeError(
            f'Failed to convert message to {message_type.__name__}: {e}',
        )


def encode(message: Message) -> str:
    """Encode message as JSON string.

    Args:
        message (Message): message to JSON encode.

    Raises:
        MessageEncodeError:
            if the message cannot be JSON encoded.
    """
    if not isinstance(message, Message):
        raise MessageEncodeError(
            f'Message is not an instance of {Message.__name__}. '
            f'Got {type(message).__name__}.',
        )

    data = dataclasses.asdict(message)
    data = uuid_to_str(data)

    try:
        return json.dumps(data)
    except TypeError as e:
        raise MessageEncodeError(f'Error encoding message: {e}')
