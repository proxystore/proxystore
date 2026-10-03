"""Build Globus Auth scopes."""

from __future__ import annotations

from collections.abc import Iterable

from globus_sdk import TransferClient
from globus_sdk.scopes import AuthScopes
from globus_sdk.scopes import GCSCollectionScopes
from globus_sdk.scopes import Scope
from globus_sdk.scopes import TransferScopes


def get_all_scopes_by_resource_server(
    collections: Iterable[str] = (),
) -> dict[str, list[Scope]]:
    """Get all scopes needed by the ProxyStore library by resource server.

    This returns scopes for two resource servers: Globus Auth and Globus
    Transfer.

    Args:
        collections: Iterable of collection UUIDs to request consent for.
            Passed to
            [`get_transfer_scopes_by_resource_server`][proxystore.globus.scopes.get_transfer_scopes_by_resource_server].
    """
    return {
        **get_auth_scopes_by_resource_server(),
        **get_transfer_scopes_by_resource_server(collections),
    }


def get_auth_scopes_by_resource_server() -> dict[str, list[Scope]]:
    """Get basic scopes for the auth API resource server."""
    return {
        AuthScopes.resource_server: [
            AuthScopes.openid,
            AuthScopes.email,
            AuthScopes.view_identity_set,
        ],
    }


def get_transfer_scopes_by_resource_server(
    collections: Iterable[str] = (),
) -> dict[str, list[Scope]]:
    """Get scopes for the transfer API resource server.

    Args:
        collections: Iterable of collection UUIDs to request consent for.
    """
    transfer_scope = TransferScopes.all

    for collection in collections:
        data_access_scope = GCSCollectionScopes(collection).data_access
        transfer_scope = transfer_scope.with_dependency(data_access_scope)

    return {TransferScopes.resource_server: [transfer_scope]}


def uses_data_access(client: TransferClient, collection: str) -> bool:
    """Check if a collection uses data access scopes.

    Args:
        client: Transfer client to use for lookup.
        collection: Collection ID to query.

    Returns:
        `True` if the collection uses a `data_access` scope and `False` \
        otherwise.
    """
    ep = client.get_endpoint(collection)
    if ep['entity_type'] != 'GCSv5_mapped_collection':
        return False
    return not ep['high_assurance']
