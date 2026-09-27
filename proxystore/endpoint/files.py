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
from typing import TypeVar

from pydantic import BaseModel
from pydantic import ValidationError

from proxystore.endpoint.exceptions import EndpointConfigError
from proxystore.serialize import BytesLike

ModelT = TypeVar('ModelT', bound=BaseModel)


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


def read_json_model(model: type[ModelT], path: str, name: str) -> ModelT:
    """Read a JSON file written by an endpoint.

    Args:
        model: Model of the file. The model should have a `version` field
            which is checked with
            [`check_format_version()`][proxystore.endpoint.files.check_format_version].
        path: Path of the file.
        name: Description of the file for error messages.

    Raises:
        FileNotFoundError: If the file does not exist.
        EndpointConfigError: If the file is malformed or has an unsupported
            format version.
    """
    with open(path, 'rb') as f:
        contents = f.read()
    try:
        return model.model_validate_json(contents, strict=True)
    except ValidationError as e:
        raise EndpointConfigError(
            f'The {name} at {path} is malformed: {e}',
        ) from None


def write_json_model(path: str, instance: BaseModel) -> None:
    """Atomically write a model to a JSON file only the owner can access.

    See [`write_private_file()`][proxystore.endpoint.files.write_private_file].
    """
    write_private_file(path, instance.model_dump_json(indent=2).encode())
