"""ProxyStore Globus Auth CLI.

```bash
# basic login/logout
proxystore-globus-auth login
proxystore-globus-auth logout
# give consent for specific collections
proxystore-globus-auth login --collection COLLECTION_UUID --collection COLLECTION_UUID ...
# specify additional scopes
proxystore-globus-auth login --scope SCOPE --scope SCOPE ...
```
"""  # noqa: E501
from __future__ import annotations

import click

from proxystore.globus.manager import NativeAppAuthManager
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
@click.option(
    '--scope',
    '-s',
    metavar='SCOPE',
    multiple=True,
    help='Additional scope to request.',
)
def login(collection: list[str], scope: list[str]) -> None:
    """Authenticate with Globus Auth.

    This requests scopes for Globus Auth, Globus Transfer, and the ProxyStore
    relay server. Collections or scopes options can be strung together. E.g.,
    request transfer scope for multiple collections with:

    $ proxystore-globus-auth -c UUID -c UUID -c UUID
    """
    basic_scopes = get_all_scopes_by_resource_server(collection)
    manager = NativeAppAuthManager(resource_server_scopes=basic_scopes)

    if manager.logged_in:
        click.echo(
            'Globus authentication tokens already exist. '
            'To recreate, logout and login again.',
        )
    else:
        manager.login(additional_scopes=scope)


@cli.command()
def logout() -> None:
    """Revoke and remove all Globus tokens."""
    manager = NativeAppAuthManager()
    manager.logout()
