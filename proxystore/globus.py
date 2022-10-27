"""Globus OAuth tools.

This implementation is based on
`Parsl's implementation <https://github.com/Parsl/parsl/blob/1.2.0/parsl/data_provider/globus.py>`_
and the
`Globus examples <https://github.com/globus/native-app-examples/blob/064569e103f7d328f3d6c4b1242234011c81dffb/example_copy_paste_refresh_token.py>`_.
"""  # noqa: E501
from __future__ import annotations

import functools
import json
import os
from typing import Any
from typing import Iterable

import globus_sdk

from proxystore.utils import home_dir

# Registered `ProxyStore Application` by jgpauloski@uchicago.edu
_APPLICATION_ID = 'a3379dba-a492-459a-a8df-5e7676a0472f'
_REDIRECT_URI = 'https://auth.globus.org/v2/web/auth-code'
_SCOPES = ('openid ', 'urn:globus:auth:scope:transfer.api.globus.org:all')
_TOKENS_FILE = 'globus-tokens.json'


class GlobusAuthFileError(Exception):
    """Exception raised if the Globus Auth token file cannot be read."""

    pass


def load_tokens_from_file(filepath: str) -> dict[str, dict[str, Any]]:
    """Load a set of saved tokens."""
    with open(filepath) as f:
        tokens = json.load(f)

    return tokens


def save_tokens_to_file(
    filepath: str,
    tokens: globus_sdk.OAuthTokenResponse,
) -> None:
    """Save a set of tokens for later use."""
    with open(filepath, 'w') as f:
        json.dump(tokens.by_resource_server, f, indent=4)


def authenticate(
    client_id: str,
    redirect_uri: str,
    requested_scopes: Iterable[str] | None = None,
) -> globus_sdk.OAuthTokenResponse:
    """Perform Native App auth flow."""
    client = globus_sdk.NativeAppAuthClient(client_id=client_id)
    client.oauth2_start_flow(
        redirect_uri=redirect_uri,
        refresh_tokens=True,
        requested_scopes=requested_scopes,
    )

    url = client.oauth2_get_authorize_url()
    print(f'Please visit the following url to authenticate:\n{url}')

    auth_code = input('Enter the auth code: ').strip()
    return client.oauth2_exchange_code_for_tokens(auth_code)


def get_authorizer(
    client_id: str,
    tokens_file: str,
    redirect_uri: str,
    requested_scopes: Iterable[str] | None = None,
) -> globus_sdk.RefreshTokenAuthorizer:
    """Get an authorizer for the Globus SDK.

    Raises:
        GlobusAuthFileError:
            if `tokens_file` cannot be parsed.
    """
    try:
        tokens = load_tokens_from_file(tokens_file)
    except OSError as e:
        raise GlobusAuthFileError(
            f'Error loading tokens from {tokens_file}: {e}.',
        )

    transfer_tokens = tokens['transfer.api.globus.org']
    auth_client = globus_sdk.NativeAppAuthClient(client_id=client_id)

    return globus_sdk.RefreshTokenAuthorizer(
        transfer_tokens['refresh_token'],
        auth_client,
        access_token=transfer_tokens['access_token'],
        expires_at=transfer_tokens['expires_at_seconds'],
        on_refresh=functools.partial(save_tokens_to_file, tokens_file),
    )


def proxystore_authenticate(
    proxystore_dir: str | None = None,
) -> None:
    """Perform auth flow for ProxyStore native app."""
    proxystore_dir = home_dir() if proxystore_dir is None else proxystore_dir
    tokens_file = os.path.join(proxystore_dir, _TOKENS_FILE)
    os.makedirs(proxystore_dir, exist_ok=True)

    tokens = authenticate(
        client_id=_APPLICATION_ID,
        redirect_uri=_REDIRECT_URI,
        requested_scopes=_SCOPES,
    )
    save_tokens_to_file(tokens_file, tokens)


def get_proxystore_authorizer(
    proxystore_dir: str | None = None,
) -> globus_sdk.RefreshTokenAuthorizer:
    """Get an authorizer for the ProxyStore native app."""
    proxystore_dir = home_dir() if proxystore_dir is None else proxystore_dir
    tokens_file = os.path.join(proxystore_dir, _TOKENS_FILE)

    return get_authorizer(
        client_id=_APPLICATION_ID,
        tokens_file=tokens_file,
        redirect_uri=_REDIRECT_URI,
        requested_scopes=_SCOPES,
    )


def main() -> int:
    """Perform Globus authentication."""
    try:
        get_proxystore_authorizer()
    except GlobusAuthFileError:
        print(
            'Performing authentication for the ProxyStore Globus Native app.',
        )
        proxystore_authenticate()
        get_proxystore_authorizer()
        print('Globus authorization complete.')
    else:
        print(
            'Globus authorization is already completed. To re-authenticate, '
            f'remove {os.path.join(home_dir(), _TOKENS_FILE)} and try again.',
        )

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
