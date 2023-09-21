"""Build Globus Auth scopes."""
from __future__ import annotations

from typing import Iterable

from globus_sdk.scopes import AuthScopes
from globus_sdk.scopes import GCSCollectionScopeBuilder
from globus_sdk.scopes import ScopeBuilder
from globus_sdk.scopes import TransferScopes

ProxyStoreRelayScopes = ScopeBuilder(
    # "ProxyStore Relay Server" application client ID
    'ebd5bbed-95e2-47cf-9c80-39e2064274bd',
    # The relay_all scope has scope ID 27b969ab-d8a4-4e31-b53d-bd899b1d8394
    known_url_scopes=['relay_all'],
)
"""ProxyStore Relay Server scopes.

Supported Scopes:

* `relay_all`
"""


def get_all_scopes_by_resource_server(
    collections: Iterable[str] = (),
) -> dict[str, list[str]]:
    """Get all scopes needed by the ProxyStore library by resource server.

    This returns scopes for three resource servers: Globus Auth, Globus
    Transfer, and the ProxyStore Relay Server.

    Args:
        collections: Iterable of collection UUIDs to request consent for.
            Passed to
            [`get_transfer_scopes_by_resource_server`][proxystore.globus.scopes.get_transfer_scopes_by_resource_server].
    """
    return {
        **get_auth_scopes_by_resource_server(),
        **get_relay_scopes_by_resource_server(),
        **get_transfer_scopes_by_resource_server(collections),
    }


def get_auth_scopes_by_resource_server() -> dict[str, list[str]]:
    """Get basic scopes for the auth API resource server."""
    return {AuthScopes.resource_server: [AuthScopes.openid, AuthScopes.email]}


def get_relay_scopes_by_resource_server() -> dict[str, list[str]]:
    """Get all scopes for the relay server by resource server."""
    return {
        ProxyStoreRelayScopes.resource_server: [
            ProxyStoreRelayScopes.relay_all,
        ],
    }


def get_transfer_scopes_by_resource_server(
    collections: Iterable[str] = (),
) -> dict[str, list[str]]:
    """Get scopes for the transfer API resource server.

    Args:
        collections: Iterable of collection UUIDs to request consent for.
    """
    transfer_scope = TransferScopes.make_mutable('all')

    for collection in collections:
        data_access_scope = GCSCollectionScopeBuilder(collection).make_mutable(
            'data_access',
        )
        transfer_scope.add_dependency(data_access_scope)

    return {TransferScopes.resource_server: [str(transfer_scope)]}
