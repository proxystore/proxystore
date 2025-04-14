"""Create Globus Service clients."""

from __future__ import annotations

from collections.abc import Iterable

import globus_sdk
from globus_sdk.globus_app import GlobusApp

from proxystore.globus.app import _APP_NAME
from proxystore.globus.app import get_client_credentials_from_env
from proxystore.globus.app import get_globus_app
from proxystore.globus.app import PROXYSTORE_GLOBUS_CLIENT_ID
from proxystore.globus.scopes import uses_data_access


def get_confidential_app_auth_client(
    client_id: str | None = None,
    client_secret: str | None = None,
) -> globus_sdk.ConfidentialAppAuthClient:
    """Create a confidential application authentication client.

    Note:
        See the [Globus SDK docs](https://globus-sdk-python.readthedocs.io/en/stable/examples/client_credentials.html#get-a-client)
        to learn how to create a confidential application and get the ID
        and secret.

    Note:
        This function will not perform the OAuth2 flow.

    Args:
        client_id: Client ID. If either `client_id` or `client_secret` is
            `None`, the values will be read from the environment using
            [`get_client_credentials_from_env()`][proxystore.globus.app.get_client_credentials_from_env].
        client_secret: Client secret.

    Returns:
        Authorization client.

    Raises:
        ValueError: if `client_id` or `client_secret` are not provided and
            one or both of `PROXYSTORE_GLOBUS_CLIENT_ID` and
            `PROXYSTORE_GLOBUS_CLIENT_SECRET` are not set.
    """
    if client_id is None or client_secret is None:
        client_id, client_secret = get_client_credentials_from_env()

    return globus_sdk.ConfidentialAppAuthClient(
        client_id=str(client_id),
        client_secret=str(client_secret),
    )


def get_native_app_auth_client(
    client_id: str = PROXYSTORE_GLOBUS_CLIENT_ID,
    app_name: str | None = None,
) -> globus_sdk.NativeAppAuthClient:
    """Create a native app authentication client.

    Note:
        This function will not perform the OAuth2 flow.

    Args:
        client_id: Application ID. Defaults to the ProxyStore application ID.
        app_name: Application name.

    Returns:
        Authorization client.
    """
    return globus_sdk.NativeAppAuthClient(
        client_id=client_id,
        app_name=_APP_NAME if app_name is None else app_name,
    )


def get_transfer_client(
    globus_app: GlobusApp | None = None,
    collections: Iterable[str] = (),
) -> globus_sdk.TransferClient:
    """Create a transfer client.

    Args:
        globus_app: [`GlobusApp`][globus_sdk.GlobusApp] used to initialize
            the transfer client. If `None`, a default
            [`UserApp`][globus_sdk.UserApp] is created using
            [`get_user_app()`][proxystore.globus.app.get_user_app].
        collections: Iterable of collection UUIDs to add dependent
            `data_access` scopes for (via
            [`add_app_data_access_scope()`][globus_sdk.TransferClient.add_app_data_access_scope].

    Returns:
        Transfer client.
    """
    if globus_app is None:
        globus_app = get_globus_app()

    client = globus_sdk.TransferClient(app=globus_app)
    data_access_collections = [
        collection
        for collection in collections
        if uses_data_access(client, collection)
    ]
    client.add_app_data_access_scope(data_access_collections)

    return client
