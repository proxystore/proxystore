"""Create Globus Auth clients."""
from __future__ import annotations

import os
import uuid

import globus_sdk

# Registered `ProxyStore Application` by jgpauloski@uchicago.edu
_PROXYSTORE_GLOBUS_APPLICATION_ID = 'a3379dba-a492-459a-a8df-5e7676a0472f'

PROXYSTORE_GLOBUS_CLIENT_ID_ENV_NAME = 'PROXYSTORE_GLOBUS_CLIENT_ID'
PROXYSTORE_GLOBUS_CLIENT_SECRET_ENV_NAME = 'PROXYSTORE_GLOBUS_CLIENT_SECRET'


def _get_client_credentials_from_env() -> tuple[str, str]:
    try:
        client_id = os.environ[PROXYSTORE_GLOBUS_CLIENT_ID_ENV_NAME]
        client_secret = os.environ[PROXYSTORE_GLOBUS_CLIENT_SECRET_ENV_NAME]
    except KeyError as e:
        raise ValueError(
            f'Both {PROXYSTORE_GLOBUS_CLIENT_ID_ENV_NAME} and '
            f'{PROXYSTORE_GLOBUS_CLIENT_SECRET_ENV_NAME} must be set to '
            'use a client identity. Either set both environment variables '
            'or unset both to use the normal login flow.',
        ) from e

    return client_id, client_secret


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
            `None`, the values will be read from the
            `PROXYSTORE_GLOBUS_CLIENT_ID` and `PROXYSTORE_GLOBUS_CLIENT_SECRET`
            environment variables.
        client_secret: Client secret.

    Returns:
        Authorization client.

    Raises:
        ValueError: if `client_id` or `client_secret` are not provided and
            one or both of `PROXYSTORE_GLOBUS_CLIENT_ID` and
            `PROXYSTORE_GLOBUS_CLIENT_SECRET` are not set.
        ValueError: if the provided `client_id` or ID read from the environment
            is not a valid UUID.
    """
    if client_id is None or client_secret is None:
        client_id, client_secret = _get_client_credentials_from_env()

    try:
        uuid.UUID(client_id)
    except ValueError as e:
        raise ValueError(
            f'Client ID "{client_id}" is not a valid UUID. '
            'Did you use the username instead of ID?',
        ) from e

    return globus_sdk.ConfidentialAppAuthClient(
        client_id=str(client_id),
        client_secret=str(client_secret),
    )


def get_native_app_auth_client(
    client_id: str = _PROXYSTORE_GLOBUS_APPLICATION_ID,
    app_name: str = 'proxystore-client',
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
        app_name=app_name,
    )


def is_client_login() -> bool:
    """Check if Globus client identity environment variables are set.

    Based on the Globus Compute SDK's
    [`is_client_login()`](https://github.com/funcx-faas/funcX/blob/8f5b59075ae6f8e8b8b13fe1b91430271f4e0c3c/compute_sdk/globus_compute_sdk/sdk/login_manager/client_login.py#L24-L38){target=_blank}.

    Returns:
        `True` if `PROXYSTORE_GLOBUS_CLIENT_ID` and \
        `PROXYSTORE_GLOBUS_CLIENT_SECRET` are set.
    """
    try:
        _get_client_credentials_from_env()
    except ValueError:
        return False
    else:
        return True
