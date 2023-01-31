"""Globus Auth Unit Tests."""
from __future__ import annotations

import contextlib
import json
import os
import pathlib
from unittest import mock

import globus_sdk
import pytest

from proxystore.globus import _get_proxystore_scopes
from proxystore.globus import _TOKENS_FILE
from proxystore.globus import authenticate
from proxystore.globus import get_authorizer
from proxystore.globus import get_proxystore_authorizer
from proxystore.globus import GlobusAuthFileError
from proxystore.globus import load_tokens_from_file
from proxystore.globus import main
from proxystore.globus import proxystore_authenticate
from proxystore.globus import save_tokens_to_file


def test_save_load_tokens(tmp_path: pathlib.Path) -> None:
    os.makedirs(tmp_path, exist_ok=True)
    tmp_file = os.path.join(tmp_path, 'globus.json')
    data = {'tokens': {'token': '123456789'}}
    with mock.patch('globus_sdk.OAuthTokenResponse'):
        tokens = globus_sdk.OAuthTokenResponse()
        tokens.by_resource_server = data  # type: ignore

    save_tokens_to_file(tmp_file, tokens)
    assert load_tokens_from_file(tmp_file) == data


def test_authenticate(capsys) -> None:
    # This test is heavily mocked so most just checks for simple errors
    with mock.patch('globus_sdk.NativeAppAuthClient'), mock.patch(
        'builtins.input',
        return_value='123456789',
    ), contextlib.redirect_stdout(
        None,
    ):
        authenticate('1234', 'https://redirect')


def test_get_authorizer(tmp_path: pathlib.Path) -> None:
    tokens = {
        'transfer.api.globus.org': {
            'refresh_token': 1234,
            'access_token': 1234,
            'expires_at_seconds': 1234,
        },
    }
    os.makedirs(tmp_path, exist_ok=True)
    filepath = os.path.join(tmp_path, 'tokens.json')
    with open(filepath, 'w') as f:
        json.dump(tokens, f)

    with mock.patch('globus_sdk.NativeAppAuthClient'), mock.patch(
        'globus_sdk.RefreshTokenAuthorizer',
    ):
        get_authorizer('client id', filepath, 'redirect uri')


def test_get_authorizer_missing_file(tmp_path: pathlib.Path) -> None:
    filepath = os.path.join(tmp_path, 'missing_file')
    with pytest.raises(GlobusAuthFileError):
        get_authorizer('client id', filepath, 'redirect uri')


@pytest.mark.parametrize(
    'collections,additional_scopes,expected',
    (
        (
            None,
            None,
            {'openid', 'urn:globus:auth:scope:transfer.api.globus.org:all'},
        ),
        (
            ['ABCD'],
            ['XYZ'],
            {
                'openid',
                (
                    'urn:globus:auth:scope:transfer.api.globus.org:all'
                    '[*https://auth.globus.org/scopes/ABCD/data_access]'
                ),
                'XYZ',
            },
        ),
        (
            ['ABCD', 'WXYZ'],
            None,
            {
                'openid',
                (
                    'urn:globus:auth:scope:transfer.api.globus.org:all'
                    '[*https://auth.globus.org/scopes/ABCD/data_access '
                    '*https://auth.globus.org/scopes/WXYZ/data_access]'
                ),
            },
        ),
    ),
)
def test_get_proxystore_scopes(
    collections: list[str] | None,
    additional_scopes: list[str] | None,
    expected: set[str],
) -> None:
    assert (
        set(_get_proxystore_scopes(collections, additional_scopes)) == expected
    )


def test_proxystore_authenticate(tmp_path: pathlib.Path) -> None:
    data = {'tokens': {'token': '123456789'}}
    with mock.patch('globus_sdk.OAuthTokenResponse'):
        tokens = globus_sdk.OAuthTokenResponse()
        tokens.by_resource_server = data  # type: ignore

    with mock.patch('proxystore.globus.authenticate', return_value=tokens):
        proxystore_authenticate(str(tmp_path))

    assert load_tokens_from_file(os.path.join(tmp_path, _TOKENS_FILE)) == data

    with mock.patch('proxystore.globus.get_authorizer'):
        get_proxystore_authorizer(str(tmp_path))


def test_main(tmp_path: pathlib.Path) -> None:
    with mock.patch('proxystore.globus.proxystore_authenticate'), mock.patch(
        'proxystore.globus.get_proxystore_authorizer',
        side_effect=[GlobusAuthFileError(), None, None],
    ), contextlib.redirect_stdout(None):
        # First will raise auth file missing error and trigger auth flow
        assert main([]) == 0
        # Second will find auth file and already exits and exists
        assert main([]) != 0


def test_main_delete(tmp_path: pathlib.Path) -> None:
    with mock.patch('proxystore.globus.home_dir', return_value=str(tmp_path)):
        assert main(['--delete']) != 0

        token_file = tmp_path / _TOKENS_FILE
        token_file.touch()

        assert main(['--delete']) == 0
        assert not token_file.exists()
