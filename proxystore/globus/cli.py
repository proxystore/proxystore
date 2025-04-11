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
    help='Globus Collection UUID to request data_access scopes for.',
)
def login(collection: list[str]) -> None:
    """Authenticate with Globus Auth.

    This requests scopes for Globus Auth, Globus Transfer, and the ProxyStore
    relay server. Collections can be strung together. E.g., request transfer
    scopes for multiple collections with:

    $ proxystore-globus-auth -c UUID -c UUID -c UUID

    Providing UUIDs for High-Assurance GCS Mapped Collections will result
    in "Unknown Scope" errors during the login flow because those Collections
    do not use data_access. Do not provide those Collection UUIDs to the CLI.
    """
    app = get_user_app()
    scopes = get_all_scopes_by_resource_server(collection)
    app.add_scope_requirements(scopes)

    if app.login_required():
        app.login()
    else:
        click.echo(
            'Globus authentication tokens already exist. '
            'To recreate, logout and login again.',
        )


@cli.command()
def logout() -> None:
    """Revoke and remove all Globus tokens."""
    app = get_user_app()
    app.logout()
