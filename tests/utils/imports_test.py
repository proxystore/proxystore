from __future__ import annotations

from typing import Any

import pytest

from proxystore.connectors.file import FileConnector
from proxystore.connectors.local import LocalConnector
from proxystore.utils.imports import get_class_path
from proxystore.utils.imports import import_class


@pytest.mark.parametrize(
    ('cls', 'expected'),
    (
        (FileConnector, 'proxystore.connectors.file.FileConnector'),
        (LocalConnector, 'proxystore.connectors.local.LocalConnector'),
    ),
)
def test_get_class_path(cls: type[Any], expected: str) -> None:
    assert get_class_path(cls) == expected


@pytest.mark.parametrize(
    ('path', 'expected'),
    (
        ('proxystore.connectors.file.FileConnector', FileConnector),
        ('proxystore.connectors.local.LocalConnector', LocalConnector),
        ('typing.Any', Any),
    ),
)
def test_import_class(path: str, expected: type[Any]) -> None:
    assert import_class(path) == expected


def test_import_class_missing_path() -> None:
    with pytest.raises(ImportError):
        import_class('FileConnector')
