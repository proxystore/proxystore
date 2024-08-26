"""ProxyStore Globus Auth CLI.

```bash
# basic login/logout
proxystore-globus-auth login
proxystore-globus-auth logout
# give consent for specific collections
proxystore-globus-auth login --collection COLLECTION_UUID --collection COLLECTION_UUID ...
```
"""  # noqa: E501

from __future__ import annotations

import click

from proxystore.globus.app import get_user_app
from proxystore.globus.client import get_native_app_auth_client
from proxystore.globus.scopes import get_all_scopes_by_resource_server


@click.group()
def cli() -> None:  # pragma: no cover
    """ProxyStore Globus Auth."""
    pass


@cli.command()
@click.option(
    '--collection',
    '-c',
    metavar='UUID',
    multiple=True,
    help='Globus Collection UUID to request transfer scopes for.',
)
def login(collection: list[str]) -> None:
    """Authenticate with Globus Auth.

    This requests scopes for Globus Auth, Globus Transfer, and the ProxyStore
    relay server. Collections can be strung together. E.g., request transfer
    scopes for multiple collections with:

    $ proxystore-globus-auth -c UUID -c UUID -c UUID
    """
    app = get_user_app()
    scopes = get_all_scopes_by_resource_server(collection)
    app.add_scope_requirements(scopes)  # type: ignore[arg-type]

    token_data = app._token_storage.get_token_data_by_resource_server()
    if all(server in token_data for server in scopes):
        click.echo(
            'Globus authentication tokens already exist. '
            'To recreate, logout and login again.',
        )
    else:
        app.run_login_flow()


@cli.command()
def logout() -> None:
    """Revoke and remove all Globus tokens."""
    app = get_user_app()
    client = get_native_app_auth_client()
    token_data = app._token_storage.get_token_data_by_resource_server()

    for server, data in token_data.items():
        client.oauth2_revoke_token(data.access_token)
        if data.refresh_token is not None:
            client.oauth2_revoke_token(data.refresh_token)
        app._token_storage.remove_token_data(server)
