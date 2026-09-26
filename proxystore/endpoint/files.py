"""Helpers for files in endpoint directories."""

from __future__ import annotations

import contextlib
import os
import tempfile

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
