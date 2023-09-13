from __future__ import annotations

from unittest import mock

import globus_sdk

from proxystore.globus.transfer import get_transfer_client_flow


@mock.patch('proxystore.globus.transfer.NativeAppAuthManager.login')
@mock.patch('proxystore.globus.transfer.NativeAppAuthManager.get_authorizer')
def test_get_transfer_client_flow_no_check_collections(
    mock_get_authorizer,
    mock_login,
) -> None:
    get_transfer_client_flow()

    mock_login.assert_called_once()
    mock_get_authorizer.assert_called_once()


@mock.patch('proxystore.globus.transfer.NativeAppAuthManager.login')
@mock.patch('proxystore.globus.transfer.NativeAppAuthManager.get_authorizer')
@mock.patch('globus_sdk.TransferClient.operation_ls')
def test_get_transfer_client_flow_no_additional_consents_required(
    mock_operation_ls,
    mock_get_authorizer,
    mock_login,
) -> None:
    get_transfer_client_flow(
        ['collection-uuid-1', 'collection-uuid-2'],
    )

    mock_login.assert_called_once()
    mock_get_authorizer.assert_called_once()

    assert mock_operation_ls.call_count == 2


@mock.patch('proxystore.globus.transfer.NativeAppAuthManager.login')
@mock.patch('proxystore.globus.transfer.NativeAppAuthManager.get_authorizer')
def test_get_transfer_client_flow_additional_consents_required(
    mock_get_authorizer,
    mock_login,
) -> None:
    class _MockTransferAPIError(globus_sdk.TransferAPIError):
        def __init__(self) -> None:
            self._info = globus_sdk.exc.ErrorInfoContainer(None)
            self._info.consent_required._has_data = True
            self._info.consent_required.required_scopes = ['scope']

    def _raise_error():
        def _error(*args, **kwargs) -> None:
            raise _MockTransferAPIError()

        return _error

    with mock.patch('click.echo'), mock.patch(
        'globus_sdk.TransferClient.operation_ls',
        new_callable=_raise_error,
    ):
        get_transfer_client_flow(
            ['collection-uuid-1', 'collection-uuid-2'],
        )

    assert mock_login.call_count == 2
    assert mock_get_authorizer.call_count == 2


@mock.patch('proxystore.globus.transfer.NativeAppAuthManager.login')
@mock.patch('proxystore.globus.transfer.NativeAppAuthManager.get_authorizer')
def test_get_transfer_client_flow_unrelated_error(
    mock_get_authorizer,
    mock_login,
) -> None:
    class _MockTransferAPIError(globus_sdk.TransferAPIError):
        def __init__(self) -> None:
            self._info = globus_sdk.exc.ErrorInfoContainer(None)
            # Not a consent required error so while raised be operation_ls
            # should be ignored
            self._info.consent_required._has_data = False

    def _raise_error():
        def _error(*args, **kwargs) -> None:
            raise _MockTransferAPIError()

        return _error

    with mock.patch('click.echo'), mock.patch(
        'globus_sdk.TransferClient.operation_ls',
        new_callable=_raise_error,
    ):
        get_transfer_client_flow(
            ['collection-uuid-1', 'collection-uuid-2'],
        )

    mock_login.assert_called_once()
    mock_get_authorizer.assert_called_once()
