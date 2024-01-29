from __future__ import annotations

import json
import uuid
from typing import Any

import pytest

from proxystore.p2p.relay.messages import decode_relay_message
from proxystore.p2p.relay.messages import encode_relay_message
from proxystore.p2p.relay.messages import PeerConnectionRequest
from proxystore.p2p.relay.messages import RelayMessage
from proxystore.p2p.relay.messages import RelayMessageDecodeError
from proxystore.p2p.relay.messages import RelayMessageEncodeError
from proxystore.p2p.relay.messages import RelayMessageType
from proxystore.p2p.relay.messages import RelayRegistrationRequest
from proxystore.p2p.relay.messages import RelayResponse
from proxystore.p2p.relay.messages import str_to_uuid
from proxystore.p2p.relay.messages import uuid_to_str

_TEST_UUID = uuid.uuid4()


@pytest.mark.parametrize(
    ('data', 'result'),
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
    assert uuid_to_str(data) == result


@pytest.mark.parametrize(
    ('data', 'result', 'exception'),
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
        with pytest.raises(RelayMessageDecodeError):
            str_to_uuid(data)
    else:
        assert str_to_uuid(data) == result


@pytest.mark.parametrize(
    'message',
    (
        RelayRegistrationRequest(name='host', uuid=uuid.uuid4()),
        RelayResponse(),
        PeerConnectionRequest(
            source_uuid=uuid.uuid4(),
            source_name='host',
            peer_uuid=uuid.uuid4(),
            description_type='answer',
            description='',
        ),
    ),
)
def test_encode_decode(message: RelayMessage) -> None:
    s = encode_relay_message(message)
    assert isinstance(s, str)
    assert decode_relay_message(s) == message


def test_decode_errors() -> None:
    # Not parsable as JSON
    with pytest.raises(
        RelayMessageDecodeError,
        match='Failed to load string as JSON',
    ):
        decode_relay_message('abcabc')

    # Missing message type key
    with pytest.raises(
        RelayMessageDecodeError,
        match='Message does not contain a message_type key',
    ):
        decode_relay_message(json.dumps({'key': 'value'}))

    # Unknown message type
    with pytest.raises(
        RelayMessageDecodeError,
        match='The message is of an unknown message type',
    ):
        decode_relay_message(
            json.dumps({'message_type': 'notarealmessagetype'}),
        )

    # Fail to expand JSON into message type object
    with pytest.raises(
        RelayMessageDecodeError,
        match='Failed to convert message to',
    ):
        decode_relay_message(
            json.dumps(
                # This fails because it is missing required keys
                {
                    'message_type': (RelayMessageType.relay_registration.name),
                },
            ),
        )


def test_encode_errors() -> None:
    with pytest.raises(RelayMessageEncodeError, match='not an instance'):
        encode_relay_message('just a string')  # type: ignore

    message = RelayRegistrationRequest(name='name', uuid=uuid.uuid4())
    # UUID is not JSONable so will raise encode error
    message.name = uuid.uuid4()  # type: ignore
    with pytest.raises(
        RelayMessageEncodeError,
        match='Error encoding message',
    ):
        encode_relay_message(message)
