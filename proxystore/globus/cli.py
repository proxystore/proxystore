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
from globus_sdk.gare import GlobusAuthorizationParameters

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
@click.option(
    '--domain',
    '-d',
    metavar='DOMAIN',
    multiple=True,
    help='Require identities from the domain.',
)
@click.option(
    '--force',
    is_flag=True,
    help='Force a login flow.',
)
def login(
    collection: tuple[str],
    domain: tuple[str],
    force: bool,
) -> None:
    """Authenticate with Globus Auth.

    This requests scopes for Globus Auth, Globus Transfer, and the ProxyStore
    relay server. Collections can be strung together. E.g., request transfer
    scopes for multiple collections with:

    $ proxystore-globus-auth -c UUID -c UUID -c UUID

    Providing UUIDs for High-Assurance GCS Mapped Collections will result
    in "Unknown Scope" errors during the login flow because those Collections
    do not use data_access. Do not provide those Collection UUIDs to the CLI.
    In this case, you may need to provide a domain for the collection:

    $ proxystore-globus-auth -d <domain>
    """
    app = get_user_app()
    scopes = get_all_scopes_by_resource_server(collection)
    app.add_scope_requirements(scopes)

    auth_params = (
        GlobusAuthorizationParameters(
            session_message=(
                'Your request requires an identity from specified domains.'
            ),
            session_required_single_domain=list(domain),
            # https://docs.globus.org/api/auth/reference/#authorization_code_grant_preferred
            prompt='login',
        )
        if len(domain) > 0
        else None
    )

    if force or app.login_required():
        app.login(auth_params=auth_params)
    else:
        click.echo(
            'Globus authentication tokens already exist. '
            'To recreate, logout and login again or rerun with --force.',
        )


@cli.command()
def logout() -> None:
    """Revoke and remove all Globus tokens."""
    app = get_user_app()
    app.logout()
