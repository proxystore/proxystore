"""Message encode/decode tests."""
from __future__ import annotations

import json
import uuid
from typing import Any

import pytest

from proxystore.p2p import messages

_TEST_UUID = uuid.uuid4()


@pytest.mark.parametrize(
    'data,result',
    (
        # Normal conversions
        ({'source_uuid': _TEST_UUID}, {'source_uuid': str(_TEST_UUID)}),
        ({'SOURCE_UUID': _TEST_UUID}, {'SOURCE_UUID': str(_TEST_UUID)}),
        (
            {'source_uuid': _TEST_UUID, 'uuid': _TEST_UUID},
            {'source_uuid': str(_TEST_UUID), 'uuid': str(_TEST_UUID)},
        ),
        # Do not convert non-UUID types
        ({'source_uuid': 1234}, {'source_uuid': 1234}),
        # Do not convert UUID types if uuid not in key
        ({'source': _TEST_UUID}, {'source': _TEST_UUID}),
        # Do not recurse into nested dicts
        ({'nested': {'uuid': _TEST_UUID}}, {'nested': {'uuid': _TEST_UUID}}),
    ),
)
def test_uuid_to_str_conversion(
    data: dict[str, Any],
    result: dict[str, Any],
) -> None:
    assert messages.uuid_to_str(data) == result


@pytest.mark.parametrize(
    'data,result,exception',
    (
        # Normal conversions
        ({'source_uuid': str(_TEST_UUID)}, {'source_uuid': _TEST_UUID}, False),
        ({'SOURCE_UUID': str(_TEST_UUID)}, {'SOURCE_UUID': _TEST_UUID}, False),
        (
            {'source_uuid': str(_TEST_UUID), 'uuid': str(_TEST_UUID)},
            {'source_uuid': _TEST_UUID, 'uuid': _TEST_UUID},
            False,
        ),
        # Fail to convert non-UUID type
        ({'source_uuid': 'abc'}, {'source_uuid': 'abc'}, True),
        ({'source_uuid': {}}, {'source_uuid': {}}, True),
        # Do not convert UUID types if uuid not in key
        ({'source': str(_TEST_UUID)}, {'source': str(_TEST_UUID)}, False),
        # Do not recurse into nested dicts
        (
            {'nested': {'uuid': str(_TEST_UUID)}},
            {'nested': {'uuid': str(_TEST_UUID)}},
            False,
        ),
    ),
)
def test_str_to_uuid_conversion(
    data: dict[str, Any],
    result: dict[str, Any],
    exception: bool,
) -> None:
    if exception:
        with pytest.raises(messages.MessageDecodeError):
            messages.str_to_uuid(data)
    else:
        assert messages.str_to_uuid(data) == result


@pytest.mark.parametrize(
    'message',
    (
        messages.ServerRegistration(name='host', uuid=uuid.uuid4()),
        messages.ServerResponse(),
        messages.PeerConnection(
            source_uuid=uuid.uuid4(),
            source_name='host',
            peer_uuid=uuid.uuid4(),
            description_type='answer',
            description='',
        ),
    ),
)
def test_encode_decode(message: messages.Message) -> None:
    s = messages.encode(message)
    assert messages.decode(s) == message


def test_decode_errors() -> None:
    # Not parsable as JSON
    with pytest.raises(
        messages.MessageDecodeError,
        match='Failed to load string as JSON',
    ):
        messages.decode('abcabc')

    # Missing message type key
    with pytest.raises(
        messages.MessageDecodeError,
        match='Message does not contain a message_type key',
    ):
        messages.decode(json.dumps({'key': 'value'}))

    # Unknown message type
    with pytest.raises(
        messages.MessageDecodeError,
        match='The message is of an unknown message type',
    ):
        messages.decode(json.dumps({'message_type': 'notarealmessagetype'}))

    # Fail to expand JSON into message type object
    with pytest.raises(
        messages.MessageDecodeError,
        match='Failed to convert message to',
    ):
        messages.decode(
            json.dumps(
                # This fails because it is missing required keys
                {
                    'message_type': (
                        messages.MessageType.server_registration.name
                    ),
                },
            ),
        )


def test_encode_errors() -> None:
    with pytest.raises(messages.MessageEncodeError, match='not an instance'):
        messages.encode('just a string')  # type: ignore

    message = messages.ServerRegistration(name='name', uuid=uuid.uuid4())
    # UUID is not JSONable so will raise encode error
    message.name = uuid.uuid4()  # type: ignore
    with pytest.raises(
        messages.MessageEncodeError,
        match='Error encoding message',
    ):
        messages.encode(message)
