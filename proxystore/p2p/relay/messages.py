"""Message types for relay client and relay server communication."""
from __future__ import annotations

import dataclasses
import enum
import json
import sys
import uuid
from typing import Any
from typing import Literal


class RelayMessageType(enum.Enum):
    """Types of messages supported."""

    relay_response = 'RelayResponse'
    """Relay response message."""
    relay_registration = 'RelayRegistrationRequest'
    """Relay registration request message."""
    peer_connection = 'PeerConnectionRequest'
    """Peer connection request message."""


@dataclasses.dataclass
class RelayMessage:
    """Base message."""

    pass


@dataclasses.dataclass
class RelayRegistrationRequest(RelayMessage):
    """Register with relay server as peer.

    Attributes:
        name: Name of peer requesting to register.
        uuid: UUID of peer requesting to register.
    """

    name: str
    uuid: uuid.UUID
    message_type: str = RelayMessageType.relay_registration.name


@dataclasses.dataclass
class RelayResponse(RelayMessage):
    """Message returned by relay server on success or error.

    Attributes:
        success: If the registration was successful.
        message: Message from server.
        error: If `message` is an error message.
    """

    success: bool = True
    message: str | None = None
    error: bool = False
    message_type: str = RelayMessageType.relay_response.name


@dataclasses.dataclass
class PeerConnectionRequest(RelayMessage):
    """Message used to request a peer-to-peer connection from a relay.

    Attributes:
        source_uuid: UUID of sending peer.
        source_name: Name of sending peer.
        peer_uuid: UUID of destination peer.
        description_type: One of `#!python 'answer'` or `#!python 'offer'`
            indicating the type of message being sent.
        description: Session description protocol message.
        error: Error string if a problem occurs.
    """

    source_uuid: uuid.UUID
    source_name: str
    peer_uuid: uuid.UUID
    description_type: Literal['answer', 'offer']
    description: str
    error: str | None = None
    message_type: str = RelayMessageType.peer_connection.name


class RelayMessageError(Exception):
    """Base exception type for relay messages."""

    pass


class RelayMessageDecodeError(RelayMessageError):
    """Exception raised when a message cannot be decoded."""

    pass


class RelayMessageEncodeError(RelayMessageError):
    """Exception raised when an message cannot be encoded."""

    pass


def uuid_to_str(data: dict[str, Any]) -> dict[str, Any]:
    """Cast any UUIDs to strings.

    Scans the input dictionary for any values where the associated key
    contains 'uuid' and value is a UUID instance and converts it to a
    string for jsonification.

    Returns:
        Shallow copy of the input dictionary with values cast from UUID \
        to str if their key also contains UUID.
    """
    data = data.copy()
    for key in data:
        if 'uuid' in key.lower() and isinstance(data[key], uuid.UUID):
            data[key] = str(data[key])
    return data


def str_to_uuid(data: dict[str, Any]) -> dict[str, Any]:
    """Cast any possible UUID strings to UUID objects.

    The inverse operation of
    [uuid_to_str()][proxystore.p2p.relay.messages.uuid_to_str].

    Returns:
        Shallow copy of the input dictionary with values cast from \
        str to UUID if the key also contains UUID.

    Raises:
        RelayMessageDecodeError: If a key contains 'uuid' but the value cannot
            be cast to a UUID.
    """
    data = data.copy()
    for key in data:
        if 'uuid' in key.lower():
            try:
                data[key] = uuid.UUID(data[key])
            except (AttributeError, TypeError, ValueError) as e:
                raise RelayMessageDecodeError(
                    f'Failed to convert key {key} to UUID.',
                ) from e
    return data


def decode_relay_message(message: str) -> RelayMessage:
    """Decode JSON string into correct relay message type.

    Args:
        message: JSON string to decode.

    Returns:
        Parsed message.

    Raises:
        RelayMessageDecodeError: If the message cannot be decoded.
    """
    try:
        data = json.loads(message)
    except json.JSONDecodeError as e:
        raise RelayMessageDecodeError('Failed to load string as JSON.') from e

    try:
        message_type_name = data.pop('message_type')
    except KeyError as e:
        raise RelayMessageDecodeError(
            'Message does not contain a message_type key.',
        ) from e

    try:
        message_type = getattr(
            sys.modules[__name__],
            RelayMessageType[message_type_name].value,
        )
    except (AttributeError, KeyError) as e:
        raise RelayMessageDecodeError(
            'The message is of an unknown message type: '
            f'{message_type_name}.',
        ) from e

    data = str_to_uuid(data)

    try:
        return message_type(**data)
    except TypeError as e:
        raise RelayMessageDecodeError(
            f'Failed to convert message to {message_type.__name__}: {e}',
        ) from e


def encode_relay_message(message: RelayMessage) -> str:
    """Encode message as JSON string.

    Args:
        message: Message to JSON encode.

    Raises:
        RelayMessageEncodeError: If the message cannot be JSON encoded.
    """
    if not isinstance(message, RelayMessage):
        raise RelayMessageEncodeError(
            f'Message is not an instance of {RelayMessage.__name__}. '
            f'Got {type(message).__name__}.',
        )

    data = dataclasses.asdict(message)
    data = uuid_to_str(data)

    try:
        return json.dumps(data)
    except TypeError as e:
        raise RelayMessageEncodeError('Error encoding message.') from e
