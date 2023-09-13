"""Build Globus Auth scopes."""
from __future__ import annotations

from typing import Iterable

from globus_sdk.scopes import AuthScopes
from globus_sdk.scopes import GCSCollectionScopeBuilder
from globus_sdk.scopes import TransferScopes


def get_auth_scopes_by_resource_server() -> dict[str, list[str]]:
    """Get basic scopes for the auth API resource server."""
    return {AuthScopes.resource_server: [AuthScopes.openid]}


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
