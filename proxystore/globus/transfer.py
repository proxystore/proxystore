"""Create and authenticate a Globus Transfer client."""
from __future__ import annotations

from typing import Iterable

import click
import globus_sdk

from proxystore.globus.manager import NativeAppAuthManager
from proxystore.globus.scopes import get_transfer_scopes_by_resource_server


def get_transfer_client_flow(
    check_collections: Iterable[str] = (),
) -> globus_sdk.TransferClient:
    """Create a transfer client with consent handling.

    Performs a transfer client creation flow. The user is first prompted to
    authenticate with Globus if the user has not already authenticated.
    Then all collections in `check_collections` are checked to make sure that
    they are accessible. If any `ConsentRequired` errors are caught, the
    user is asked to re-authenticate with the required additional scopes.

    Args:
        check_collections: An iterable of collection UUIDs to ensure the
            user's tokens have consent for.

    Returns:
        Transfer client.
    """
    scopes = get_transfer_scopes_by_resource_server()
    manager = NativeAppAuthManager(resource_server_scopes=scopes)
    manager.login()

    authorizer = manager.get_authorizer(
        globus_sdk.scopes.TransferScopes.resource_server,
    )
    transfer_client = globus_sdk.TransferClient(authorizer=authorizer)

    consent_required_scopes: list[str] = []
    for collection in check_collections:
        try:
            transfer_client.operation_ls(collection, path='/')
        except globus_sdk.TransferAPIError as e:
            if (
                e.info.consent_required
                and e.info.consent_required.required_scopes is not None
            ):
                consent_required_scopes.extend(
                    e.info.consent_required.required_scopes,
                )

    if len(consent_required_scopes) == 0:
        return transfer_client

    click.echo(
        'One or more collections require consent in order to be used. '
        'Login again to to grant the remaining consents.',
    )
    manager.login(additional_scopes=consent_required_scopes)
    authorizer = manager.get_authorizer(
        globus_sdk.scopes.TransferScopes.resource_server,
    )
    return globus_sdk.TransferClient(authorizer=authorizer)
