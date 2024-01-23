"""Globus Auth credential managers."""
from __future__ import annotations

import platform
from typing import Iterable
from typing import Protocol
from typing import runtime_checkable

import click
import globus_sdk

from proxystore.globus.client import get_confidential_app_auth_client
from proxystore.globus.client import get_native_app_auth_client
from proxystore.globus.scopes import get_all_scopes_by_resource_server
from proxystore.globus.storage import get_token_storage_adapter


@runtime_checkable
class GlobusAuthManager(Protocol):
    """Protocol for a Globus Auth manager."""

    @property
    def client(self) -> globus_sdk.AuthLoginClient:
        """Globus Auth client."""
        ...

    @property
    def logged_in(self) -> bool:
        """User has valid refresh tokens for necessary scopes."""
        ...

    def get_authorizer(
        self,
        resource_server: str,
    ) -> globus_sdk.authorizers.GlobusAuthorizer:
        """Get authorizer for a specific resource server.

        Raises:
            LookupError: if tokens for the resource server do not exist.
        """
        ...

    def login(self, *, additional_scopes: Iterable[str] = ()) -> None:
        """Perform the authentication flow.

        This method is idempotent meaning it will be a no-op if the user
        is already logged in.

        Args:
            additional_scopes: Additional scopes to request.
        """
        ...

    def logout(self) -> None:
        """Revoke and remove authentication tokens."""
        ...


class ConfidentialAppAuthManager:
    """Globus confidential app (client identity) credential manager.

    Args:
        client: Optionally override the standard ProxyStore auth client.
        storage: Optionally override the default token storage.
        resource_server_scopes: Mapping of resource server URLs to a list
            of scopes for that resource server. If unspecified, all basic
            scopes needed by ProxyStore components will be requested.
            This parameter can be used to request scopes for many resource
            server when
            [`login()`][proxystore.globus.manager.ConfidentialAppAuthManager.login]
            is invoked.
    """

    def __init__(
        self,
        *,
        client: globus_sdk.ConfidentialAppAuthClient | None = None,
        storage: globus_sdk.tokenstorage.SQLiteAdapter | None = None,
        resource_server_scopes: dict[str, list[str]] | None = None,
    ) -> None:
        self._client = (
            client
            if client is not None
            else get_confidential_app_auth_client()
        )
        self._storage = (
            storage
            if storage is not None
            else get_token_storage_adapter(
                namespace=f'client/{self._client.client_id}',
            )
        )
        self._resource_server_scopes = (
            resource_server_scopes
            if resource_server_scopes is not None
            else get_all_scopes_by_resource_server()
        )

    @property
    def client(self) -> globus_sdk.ConfidentialAppAuthClient:
        """Globus Auth client."""
        return self._client

    @property
    def logged_in(self) -> bool:
        """User has valid refresh tokens for necessary scopes.

        This is always true for client identities.
        """
        return True

    def get_authorizer(
        self,
        resource_server: str,
    ) -> globus_sdk.authorizers.GlobusAuthorizer:
        """Get authorizer for a specific resource server."""
        scopes = []
        for rs_name, rs_scopes in self._resource_server_scopes.items():
            if rs_name == resource_server:
                scopes.extend(rs_scopes)

        tokens = self._storage.get_token_data(resource_server)
        if tokens is None:
            tokens = {}

        return globus_sdk.ClientCredentialsAuthorizer(
            confidential_client=self.client,
            scopes=scopes,
            access_token=tokens.get('access_token', None),
            expires_at=tokens.get('expires_at_seconds', None),
            on_refresh=self._storage.on_refresh,
        )

    def login(self, *, additional_scopes: Iterable[str] = ()) -> None:
        """Perform the authentication flow.

        Client identities do not require a login flow so this is a no-op.

        Args:
            additional_scopes: Additional scopes to request.
        """
        return

    def logout(self) -> None:
        """Revoke and remove authentication tokens."""
        for server, data in self._storage.get_by_resource_server().items():
            for key in ('access_token', 'refresh_token'):
                token = data[key]
                self.client.oauth2_revoke_token(token)
            self._storage.remove_tokens_for_resource_server(server)


class NativeAppAuthManager:
    """Globus native app credential manager.

    Args:
        client: Optionally override the standard ProxyStore auth client.
        storage: Optionally override the default token storage.
        resource_server_scopes: Mapping of resource server URLs to a list
            of scopes for that resource server. If unspecified, all basic
            scopes needed by ProxyStore components will be requested.
            This parameter can be used to request scopes for many resource
            server when
            [`login()`][proxystore.globus.manager.NativeAppAuthManager.login]
            is invoked.
    """

    def __init__(
        self,
        *,
        client: globus_sdk.NativeAppAuthClient | None = None,
        storage: globus_sdk.tokenstorage.SQLiteAdapter | None = None,
        resource_server_scopes: dict[str, list[str]] | None = None,
    ) -> None:
        self._client = (
            client if client is not None else get_native_app_auth_client()
        )
        self._storage = (
            storage
            if storage is not None
            else get_token_storage_adapter(
                namespace=f'user/{self._client.client_id}',
            )
        )
        self._resource_server_scopes = (
            resource_server_scopes
            if resource_server_scopes is not None
            else get_all_scopes_by_resource_server()
        )

    @property
    def client(self) -> globus_sdk.NativeAppAuthClient:
        """Globus Auth client."""
        return self._client

    @property
    def logged_in(self) -> bool:
        """User has valid refresh tokens for necessary scopes."""
        data = self._storage.get_by_resource_server()
        return all(server in data for server in self._resource_server_scopes)

    def get_authorizer(
        self,
        resource_server: str,
    ) -> globus_sdk.authorizers.GlobusAuthorizer:
        """Get authorizer for a specific resource server.

        Raises:
            LookupError: if tokens for the resource server do not exist.
        """
        tokens = self._storage.get_token_data(resource_server)
        if tokens is None:
            raise LookupError(f'Could not find tokens for {resource_server}.')
        return globus_sdk.RefreshTokenAuthorizer(
            tokens['refresh_token'],
            self.client,
            access_token=tokens['access_token'],
            expires_at=tokens['expires_at_seconds'],
            on_refresh=self._storage.on_refresh,
        )

    def _run_login_flow(
        self,
        *,
        additional_scopes: Iterable[str] = (),
    ) -> globus_sdk.OAuthTokenResponse:
        # Flatten required scopes for the resource servers this manager
        # was initialized with
        scopes = [
            scope
            for rs_scopes in self._resource_server_scopes.values()
            for scope in rs_scopes
        ]
        scopes.extend(additional_scopes)

        network_name = platform.node().replace(' ', '-')
        self.client.oauth2_start_flow(
            refresh_tokens=True,
            requested_scopes=scopes,
            prefill_named_grant=f'{self.client.app_name}-{network_name}',
        )

        url = self.client.oauth2_get_authorize_url()
        click.secho(
            'Please visit the following url to authenticate:',
            fg='cyan',
        )
        click.echo(url)

        auth_code = click.prompt(
            click.style('Enter the auth code:', fg='cyan'),
            prompt_suffix=' ',
        )
        auth_code = auth_code.strip()

        tokens = self.client.oauth2_exchange_code_for_tokens(auth_code)
        click.echo('Globus authentication completed.')
        return tokens

    def login(self, *, additional_scopes: Iterable[str] = ()) -> None:
        """Perform the authentication flow.

        This method is idempotent meaning it will be a no-op if the user
        is already logged in.

        On log in, the user will be prompted to follow a link to authenticate
        on [globus.org](https://globus.org).

        Args:
            additional_scopes: Additional scopes to request.
        """
        if not self.logged_in:
            token = self._run_login_flow(additional_scopes=additional_scopes)
            self._storage.store(token)

    def logout(self) -> None:
        """Revoke and remove authentication tokens."""
        for server, data in self._storage.get_by_resource_server().items():
            for key in ('access_token', 'refresh_token'):
                token = data[key]
                self.client.oauth2_revoke_token(token)
            self._storage.remove_tokens_for_resource_server(server)
