"""Create [`GlobusApp`][globus_sdk.GlobusApp] instances."""

from __future__ import annotations

import os
import pathlib

import click
from globus_sdk.globus_app import ClientApp
from globus_sdk.globus_app import GlobusApp
from globus_sdk.globus_app import GlobusAppConfig
from globus_sdk.globus_app import UserApp
from globus_sdk.login_flows import CommandLineLoginFlowManager
from globus_sdk.tokenstorage import SQLiteTokenStorage

from proxystore.utils.environment import home_dir

# Registered `ProxyStore Application` by jgpauloski@uchicago.edu
PROXYSTORE_GLOBUS_CLIENT_ID = 'a3379dba-a492-459a-a8df-5e7676a0472f'

PROXYSTORE_GLOBUS_CLIENT_ID_ENV_NAME = 'PROXYSTORE_GLOBUS_CLIENT_ID'
PROXYSTORE_GLOBUS_CLIENT_SECRET_ENV_NAME = 'PROXYSTORE_GLOBUS_CLIENT_SECRET'

_APP_NAME = 'proxystore'
_TOKENS_FILE = 'storage.db'


class _CustomLoginFlowManager(CommandLineLoginFlowManager):
    def print_authorize_url(
        self,
        authorize_url: str,
    ) -> None:  # pragma: no cover
        click.secho(
            'Please visit the following url to authenticate:',
            fg='cyan',
        )
        click.echo(authorize_url)

    def prompt_for_code(self) -> str:  # pragma: no cover
        auth_code = click.prompt(
            click.style('Enter the auth code:', fg='cyan'),
            prompt_suffix=' ',
        )
        return auth_code.strip()


def get_token_storage(
    filepath: str | pathlib.Path | None = None,
    *,
    namespace: str = 'DEFAULT',
) -> SQLiteTokenStorage:
    """Create token storage adapter.

    Args:
        filepath: Name of the database file. If not provided, defaults to a
            file in the ProxyStore home directory
            (see [`home_dir()`][proxystore.utils.environment.home_dir]).
        namespace: Optional namespace to use within the database for
            partitioning token data.

    Returns:
        Token storage.
    """
    if filepath is None:
        filepath = os.path.join(home_dir(), _TOKENS_FILE)
    filepath = pathlib.Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    return SQLiteTokenStorage(filepath, namespace=namespace)


def get_client_credentials_from_env() -> tuple[str, str]:
    """Read the Globus Client ID and secret from the environment.

    The Client ID should be set to `PROXYSTORE_GLOBUS_CLIENT_ID` and
    the secret to `PROXYSTORE_GLOBUS_CLIENT_SECRET`.

    Note:
        This function performs no validation on the values of the variables.

    Returns:
        Tuple containing the client ID and secret.

    Raises:
        ValueError: if one of the environment variables is set.
    """
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


def get_globus_app() -> GlobusApp:
    """Get a Globus App based on the environment.

    If a client ID and secret are set in the environment, returns a
    [`ClientApp`][globus_sdk.ClientApp] using
    [`get_client_app()`][proxystore.globus.app.get_client_app]. Otherwise
    returns a [`UserApp`][globus_sdk.UserApp] using
    [`get_user_app()`][proxystore.globus.app.get_user_app].

    Returns:
        Initialized app.
    """
    if is_client_login():
        return get_client_app()
    return get_user_app()


def get_client_app(
    client_id: str | None = None,
    client_secret: str | None = None,
) -> ClientApp:
    """Get a Client Globus App.

    Args:
        client_id: Client ID. If one or both of the `client_id` and
            `client_secret` are not provided, the values will be read from
            the environment using
            [`get_client_credentials_from_env()`][proxystore.globus.app.get_client_credentials_from_env].
        client_secret: Client secret. See above.

    Returns:
        Initialized app.
    """
    if client_id is None or client_secret is None:
        client_id, client_secret = get_client_credentials_from_env()

    config = GlobusAppConfig(
        token_storage=get_token_storage(),
        request_refresh_tokens=True,
    )

    return ClientApp(
        app_name=_APP_NAME,
        client_id=client_id,
        client_secret=client_secret,
        config=config,
    )


def get_user_app() -> UserApp:
    """Get a User Globus App.

    The [`UserApp`][globus_sdk.UserApp] will
    automatically perform an interactive flow with the user as needed.

    Returns:
        Initialized app.
    """
    config = GlobusAppConfig(
        login_flow_manager=_CustomLoginFlowManager,
        token_storage=get_token_storage(),
        request_refresh_tokens=True,
    )

    return UserApp(
        app_name=_APP_NAME,
        client_id=PROXYSTORE_GLOBUS_CLIENT_ID,
        config=config,
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
        get_client_credentials_from_env()
    except ValueError:
        return False
    else:
        return True
