"""Helpers for files in endpoint directories.

Warning:
    This module is an internal implementation detail. Its interface may
    change between releases without notice (see
    [`proxystore.endpoint`][proxystore.endpoint]).
"""

from __future__ import annotations

import contextlib
import os
import tempfile
from typing import Any

from proxystore.serialize import BytesLike


def write_private_file(path: str, data: BytesLike) -> None:
    """Atomically write data to a file that only the owner can access.

    The data is written to a temporary file with mode `0600` in the same
    directory which then replaces `path`, so readers never observe a
    partially written file.
    """
    fd, tmp_path = tempfile.mkstemp(
        dir=os.path.dirname(path) or '.',
        prefix=f'.{os.path.basename(path)}.',
    )
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(data)
        os.replace(tmp_path, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.remove(tmp_path)
        raise


def check_format_version(version: Any, supported: int, name: str) -> int:
    """Check the format version of a file in an endpoint directory.

    Each file written by ProxyStore has a format version which is incremented
    on incompatible changes to the file format.

    Args:
        version: Version in the file.
        supported: Version supported by this version of ProxyStore.
        name: Description of the file for the error message.

    Returns:
        The version.

    Raises:
        ValueError: If the version is not the supported version.
    """
    if version != supported:
        raise ValueError(
            f'The {name} has format version {version!r}, but this version '
            f'of ProxyStore only supports version {supported}. The file was '
            'likely written by a different version of ProxyStore.',
        )
    return version
