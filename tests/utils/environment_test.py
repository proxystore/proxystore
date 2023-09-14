from __future__ import annotations

import os
from unittest import mock

import pytest

from proxystore.utils.environment import home_dir


@pytest.mark.parametrize(
    ('env', 'expected'),
    (
        ({}, '~/.local/share/proxystore'),
        ({'PROXYSTORE_HOME': '/path/to/home'}, '/path/to/home'),
        ({'XDG_DATA_HOME': '/path/to/home'}, '/path/to/home/proxystore'),
        ({'PROXYSTORE_HOME': '/home1', 'XDG_DATA_HOME': '/home2'}, '/home1'),
    ),
)
def test_home_dir(env: dict[str, str], expected: str) -> None:
    # Unset $XDG_DATA_HOME and save for restoring later
    xdg_data_home = os.environ.pop('XDG_DATA_HOME', None)

    with mock.patch.dict(os.environ, env):
        path = home_dir()
        assert isinstance(path, str)
        assert os.path.isabs(path)
        assert path == os.path.expanduser(expected)

    # Exclude from coverage because not every OS will set $XDG_DATA_HOME
    if xdg_data_home is not None:  # pragma: no cover
        os.environ['XDG_DATA_HOME'] = xdg_data_home
