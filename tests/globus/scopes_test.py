from __future__ import annotations

import uuid
from typing import Any
from unittest import mock

import pytest
from globus_sdk.scopes import AuthScopes
from globus_sdk.scopes import TransferScopes

from proxystore.globus.scopes import get_all_scopes_by_resource_server
from proxystore.globus.scopes import get_auth_scopes_by_resource_server
from proxystore.globus.scopes import get_relay_scopes_by_resource_server
from proxystore.globus.scopes import get_transfer_scopes_by_resource_server
from proxystore.globus.scopes import ProxyStoreRelayScopes
from proxystore.globus.scopes import uses_data_access
from testing.mocked.globus import MockTransferClient


def test_get_all_scopes_by_resource_server() -> None:
    scopes = get_all_scopes_by_resource_server()
    assert AuthScopes.resource_server in scopes
    assert ProxyStoreRelayScopes.resource_server in scopes
    assert TransferScopes.resource_server in scopes


def test_get_auth_scopes_by_resource_server() -> None:
    scopes = get_auth_scopes_by_resource_server()
    expected = {
        AuthScopes.resource_server: [
            AuthScopes.openid,
            AuthScopes.email,
            AuthScopes.view_identity_set,
        ],
    }
    assert scopes == expected


def test_get_relay_scopes_by_resource_server() -> None:
    scopes = get_relay_scopes_by_resource_server()
    expected = {
        ProxyStoreRelayScopes.resource_server: [
            ProxyStoreRelayScopes.relay_all,
        ],
    }
    assert scopes == expected


def test_get_transfer_scopes_by_resource_server() -> None:
    scopes = get_transfer_scopes_by_resource_server()
    expected = {TransferScopes.resource_server: [TransferScopes.all]}
    assert scopes == expected


def test_get_transfer_scopes_by_resource_server_with_collections() -> None:
    collections = ['ABC', 'XYZ']
    urls = [
        f'https://auth.globus.org/scopes/{collection}/data_access'
        for collection in collections
    ]

    scopes = get_transfer_scopes_by_resource_server(collections)
    expected = {
        TransferScopes.resource_server: [
            f'{TransferScopes.all}[{" ".join(urls)}]',
        ],
    }
    assert scopes == expected


@pytest.mark.parametrize(
    ('data', 'expected'),
    (
        ({'entity_type': 'GCSv4'}, False),
        (
            {'entity_type': 'GCSv5_mapped_collection', 'high_assurance': True},
            False,
        ),
        (
            {
                'entity_type': 'GCSv5_mapped_collection',
                'high_assurance': False,
            },
            True,
        ),
    ),
)
def test_use_data_access(data: dict[str, Any], expected: bool) -> None:
    client = MockTransferClient()
    with mock.patch.object(client, 'get_endpoint', return_value=data):
        result = uses_data_access(client, str(uuid.uuid4()))  # type: ignore[arg-type]
        assert result == expected
