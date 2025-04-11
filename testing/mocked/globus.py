"""Mock classes for the Globus SDK."""

from __future__ import annotations

import uuid
from typing import Any
from unittest import mock

import globus_sdk
from globus_sdk.globus_app import GlobusAppConfig
from globus_sdk.globus_app import UserApp
from globus_sdk.tokenstorage import MemoryTokenStorage


class MockTransferData(globus_sdk.TransferData):
    """Mock the Globus TransferData."""

    def __init__(self, *args, **kwargs):
        pass

    def __setitem__(self, key, item):
        self.__dict__[key] = item

    def add_item(
        self,
        source_path: str,
        destination_path: str,
        **kwargs: Any,
    ) -> None:
        """Add item."""
        assert isinstance(source_path, str)
        assert isinstance(destination_path, str)


class MockDeleteData(globus_sdk.DeleteData):
    """Mock the Globus DeleteData."""

    def __init__(self, *args, **kwargs):
        pass

    def __setitem__(self, key, item):
        self.__dict__[key] = item

    def add_item(self, path: str, **kwargs: Any) -> None:
        """Add an item to the object."""
        assert isinstance(path, str)


class MockTransferClient:
    """Mock the Globus TransferClient."""

    def __init__(self, *args, **kwargs):
        pass

    def get_endpoint(self, *args, **kwargs) -> Any:  # pragma: no cover
        pass

    def get_task(self, task_id: str) -> Any:
        """Get task."""
        assert isinstance(task_id, str)
        return None

    def submit_delete(self, delete_data: MockDeleteData) -> dict[str, str]:
        """Submit DeleteData."""
        assert isinstance(delete_data, MockDeleteData)
        return {'task_id': str(uuid.uuid4())}

    def submit_transfer(
        self,
        transfer_data: MockTransferData,
    ) -> dict[str, str]:
        """Submit TransferData."""
        assert isinstance(transfer_data, MockTransferData)
        return {'task_id': str(uuid.uuid4())}

    def task_wait(self, task_id: str, **kwargs: Any) -> bool:
        """Wait on tasks."""
        assert isinstance(task_id, str)
        return True


def get_testing_app() -> UserApp:
    config = GlobusAppConfig(token_storage=MemoryTokenStorage())
    mock_client = mock.Mock(
        spec=globus_sdk.NativeAppAuthClient,
        client_id='mock-client_id',
        base_url='https://auth.globus.org',
        environment='production',
    )
    return UserApp(
        app_name='test-app',
        login_client=mock_client,
        config=config,
    )
