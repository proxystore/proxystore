"""Globus OAuth tools.

ProxyStore provides the `proxystore-globus-auth` CLI tool to give consent
to the ProxyStore Globus Application.

```bash
# basic authentication
proxystore-globus-auth
# delete old tokens
proxystore-globus-auth --delete
# give consent for specific collections
proxystore-globus-auth --collections COLLECTION_UUID COLLECTION_UUID ...
# specify additional scopes
proxystore-globus-auth --scopes SCOPE SCOPE ...
```

Based on [Parsl's implementation](https://github.com/Parsl/parsl/blob/1.2.0/parsl/data_provider/globus.py){target=_blank}
and the [Globus examples](https://github.com/globus/native-app-examples/blob/064569e103f7d328f3d6c4b1242234011c81dffb/example_copy_paste_refresh_token.py){target=_blank}.
"""
from __future__ import annotations

import functools
import json
import os
from typing import Any
from typing import Iterable

import click
import globus_sdk

from proxystore.utils import home_dir

# Registered `ProxyStore Application` by jgpauloski@uchicago.edu
_APPLICATION_ID = 'a3379dba-a492-459a-a8df-5e7676a0472f'
# https://docs.globus.org/globus-connect-server/migrating-to-v5.4/application-migration/#activation_is_replaced_by_consent  # noqa: E501
_COLLECTION_CONSENT = (
    '*https://auth.globus.org/scopes/{COLLECTION}/data_access'
)
_REDIRECT_URI = 'https://auth.globus.org/v2/web/auth-code'
_TOKENS_FILE = 'globus-tokens.json'


class GlobusAuthFileError(Exception):
    """Exception raised if the Globus Auth token file cannot be read."""

    pass


def load_tokens_from_file(filepath: str) -> dict[str, dict[str, Any]]:
    """Load a set of saved tokens.

    Args:
        filepath: Filepath containing JSON tokens to load.

    Returns:
        JSON data from tokens file.
    """
    with open(filepath) as f:
        return json.load(f)


def save_tokens_to_file(
    filepath: str,
    tokens: globus_sdk.OAuthTokenResponse,
) -> None:
    """Save a set of tokens for later use.

    Args:
        filepath: Filepath to write tokens to.
        tokens: Tokens returned by the Globus API.
    """
    with open(filepath, 'w') as f:
        json.dump(tokens.by_resource_server, f, indent=4)


def authenticate(
    client_id: str,
    redirect_uri: str | None = None,
    requested_scopes: Iterable[str] | None = None,
) -> globus_sdk.OAuthTokenResponse:
    """Perform Native App auth flow.

    This will print a link to `auth.globus.org` where the user will
    continue the authentication process. Then the function will wait on
    the user to input the authorization code.

    Args:
        client_id: Globus app ID.
        redirect_uri: The page to direct users to after authentication.
        requested_scopes: Iterable of scopes on the token being requested.

    Returns:
        Tokens returned by the Globus API.
    """
    client = globus_sdk.NativeAppAuthClient(client_id=client_id)
    client.oauth2_start_flow(
        redirect_uri=redirect_uri,
        refresh_tokens=True,
        requested_scopes=requested_scopes,
    )

    url = client.oauth2_get_authorize_url()
    click.secho('Please visit the following url to authenticate:', fg='cyan')
    click.echo(url)

    auth_code = click.prompt(
        click.style('Enter the auth code:', fg='cyan'),
        prompt_suffix=' ',
    )
    auth_code = auth_code.strip()
    return client.oauth2_exchange_code_for_tokens(auth_code)


def get_authorizer(
    client_id: str,
    tokens_file: str,
) -> globus_sdk.RefreshTokenAuthorizer:
    """Get an authorizer for the Globus SDK.

    Args:
        client_id: Globus app ID.
        tokens_file: Filepath to saved Globus Auth tokens.

    Returns:
        Authorizer than can be used with other parts of the Globus SDK.

    Raises:
        GlobusAuthFileError: If `tokens_file` cannot be parsed.
    """
    try:
        tokens = load_tokens_from_file(tokens_file)
    except OSError as e:
        raise GlobusAuthFileError(
            f'Error loading tokens from {tokens_file}: {e}.',
        ) from e

    transfer_tokens = tokens['transfer.api.globus.org']
    auth_client = globus_sdk.NativeAppAuthClient(client_id=client_id)

    return globus_sdk.RefreshTokenAuthorizer(
        transfer_tokens['refresh_token'],
        auth_client,
        access_token=transfer_tokens['access_token'],
        expires_at=transfer_tokens['expires_at_seconds'],
        on_refresh=functools.partial(save_tokens_to_file, tokens_file),
    )


def _get_proxystore_scopes(
    collections: Iterable[str] | None = None,
    additional_scopes: Iterable[str] | None = None,
) -> list[str]:
    scopes = ['openid']

    transfer_scope = 'urn:globus:auth:scope:transfer.api.globus.org:all'
    if collections is not None:
        data_access = [
            _COLLECTION_CONSENT.format(COLLECTION=c) for c in collections
        ]
        transfer_scope = f'{transfer_scope}[{" ".join(data_access)}]'
    scopes.append(transfer_scope)

    if additional_scopes is not None:
        scopes.extend(additional_scopes)

    return scopes


def proxystore_authenticate(
    proxystore_dir: str | None = None,
    collections: list[str] | None = None,
    additional_scopes: list[str] | None = None,
) -> str:
    """Perform auth flow for ProxyStore native app.

    This is a wrapper around [`authenticate()`][proxystore.globus.authenticate]
    which stores tokens in the ProxyStore home directory and requests the
    appropriate scopes for ProxyStore.

    Alert:
        Globus Connect Server v5 uses consents rather than activations so
        users need to consent to the Transfer service accessing the
        specific mapped collection on behalf of the user. Read more
        [here](https://docs.globus.org/globus-connect-server/migrating-to-v5.4/application-migration/#activation_is_replaced_by_consent){target=_blank}.

    Args:
        proxystore_dir: Optionally specify the proxystore home directory.
            Defaults to [`home_dir()`][proxystore.utils.home_dir].
        collections: Globus Collection UUIDs to request transfer scopes for.
        additional_scopes: Extra scopes to include in the authorization
            request.

    Returns:
        Path to saved tokens file.
    """
    proxystore_dir = home_dir() if proxystore_dir is None else proxystore_dir
    tokens_file = os.path.join(proxystore_dir, _TOKENS_FILE)
    os.makedirs(proxystore_dir, exist_ok=True)

    scopes = _get_proxystore_scopes(collections, additional_scopes)

    tokens = authenticate(
        client_id=_APPLICATION_ID,
        redirect_uri=_REDIRECT_URI,
        requested_scopes=scopes,
    )
    save_tokens_to_file(tokens_file, tokens)
    return tokens_file


def get_proxystore_authorizer(
    proxystore_dir: str | None = None,
) -> globus_sdk.RefreshTokenAuthorizer:
    """Get an authorizer for the ProxyStore native app.

    [`proxystore_authenticate()`][proxystore.globus.proxystore_authenticate]
    or the CLI `#!bash proxystore-globus-auth` should be performed prior to
    calling this function to ensure tokens have been acquired.

    Args:
        proxystore_dir: Optionally specify the proxystore home directory.
            Defaults to [`home_dir()`][proxystore.utils.home_dir].

    Returns:
        Authorizer than can be used with other parts of the Globus SDK.
    """
    proxystore_dir = home_dir() if proxystore_dir is None else proxystore_dir
    tokens_file = os.path.join(proxystore_dir, _TOKENS_FILE)

    return get_authorizer(client_id=_APPLICATION_ID, tokens_file=tokens_file)


@click.command()
@click.option(
    '--collection',
    '-c',
    metavar='UUID',
    multiple=True,
    help='Globus Collection UUID to request transfer scopes for.',
)
@click.option(
    '--scope',
    '-s',
    metavar='SCOPE',
    multiple=True,
    help='Additional scope to request.',
)
@click.option(
    '--delete',
    is_flag=True,
    default=False,
    help='Delete existing tokens.',
)
def cli(collection: list[str], scope: list[str], delete: bool) -> None:
    """Perform Globus authentication for the Transfer service.

    Collections or scopes options can be strung together. E.g.,
    request transfer scope for multiple collections with:

    $ proxystore-globus-auth -c UUID -c UUID -c UUID
    """
    if delete:
        tokens_file = os.path.join(home_dir(), _TOKENS_FILE)
        fp = click.format_filename(tokens_file)
        if os.path.exists(tokens_file):
            os.remove(tokens_file)
            click.echo(f'Deleted tokens file: {fp}')
            return
        else:
            click.echo(f'Tokens file does not exist: {fp}')
            raise SystemExit(1)

    try:
        get_proxystore_authorizer()
    except GlobusAuthFileError:
        tokens_file = proxystore_authenticate(
            collections=collection,
            additional_scopes=scope,
        )
        get_proxystore_authorizer()
        click.echo(f'Tokens saved to: {click.format_filename(tokens_file)}')
    else:
        click.echo(
            'Globus authorization is already completed.\n\n'
            'To re-authenticate, delete your tokens and try again.\n'
            '  $ proxystore-globus-auth --delete',
        )
