"""Utilities related to the current execution environment."""
from __future__ import annotations

import os
import socket


def home_dir() -> str:
    """Return the absolute path to the proxystore home directory.

    If set, `$PROXYSTORE_HOME` is preferred. Otherwise,
    `$XDG_DATA_HOME/proxystore` is returned where `$XDG_DATA_HOME` defaults
    to `$HOME/.local/share` if unset.
    """
    path = os.environ.get('PROXYSTORE_HOME')
    if path is None:
        prefix = os.environ.get('XDG_DATA_HOME') or os.path.expanduser(
            '~/.local/share',
        )
        path = os.path.join(prefix, 'proxystore')
    return os.path.abspath(path)


def hostname() -> str:
    """Return current hostname."""
    return socket.gethostname()
