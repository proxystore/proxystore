from __future__ import annotations

from globus_sdk.scopes import AuthScopes
from globus_sdk.scopes import TransferScopes

from proxystore.globus.scopes import get_auth_scopes_by_resource_server
from proxystore.globus.scopes import get_transfer_scopes_by_resource_server


def test_get_auth_scopes_by_resource_server() -> None:
    scopes = get_auth_scopes_by_resource_server()
    expected = {AuthScopes.resource_server: [AuthScopes.openid]}
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
