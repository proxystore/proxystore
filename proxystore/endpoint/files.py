"""Helpers for files in endpoint directories."""

from __future__ import annotations

import contextlib
import os
import tempfile
import tomllib
from typing import ClassVar
from typing import TypeVar

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator

from proxystore.endpoint.exceptions import EndpointConfigError
from proxystore.serialize import BytesLike
from proxystore.utils.config import dumps

FileT = TypeVar('FileT', bound='VersionedFile')


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


class VersionedFile(BaseModel):
    """Base model of a file in an endpoint directory.

    Each file written by ProxyStore has a format version which is incremented
    on incompatible changes to the file format. Subclasses set the default of
    `version` to the version supported by this version of ProxyStore, and a
    file with any other version is rejected.

    Example:
        ```python
        class MyFile(VersionedFile):
            DESCRIPTION: ClassVar[str] = 'my file'

            version: int = 1
            value: str
        ```

    Attributes:
        DESCRIPTION: Description of the file for error messages.
        version: Format version of the file.
    """

    model_config = ConfigDict(extra='forbid')

    DESCRIPTION: ClassVar[str]

    version: int

    @field_validator('version')
    @classmethod
    def _version_validator(cls, v: int) -> int:
        supported = cls.model_fields['version'].default
        if v != supported:
            raise ValueError(
                f'The {cls.DESCRIPTION} has format version {v!r}, but this '
                f'version of ProxyStore only supports version {supported}. '
                'The file was likely written by a different version of '
                'ProxyStore.',
            )
        return v


def read_model(model: type[FileT], path: str) -> FileT:
    """Read a file written by an endpoint.

    The file is parsed as TOML if `path` ends with `.toml` and as JSON
    otherwise.

    Raises:
        FileNotFoundError: If the file does not exist.
        EndpointConfigError: If the file is malformed or has an unsupported
            format version.
    """
    with open(path, 'rb') as f:
        contents = f.read()
    try:
        if path.endswith('.toml'):
            return model.model_validate(
                tomllib.loads(contents.decode()),
                strict=True,
            )
        return model.model_validate_json(contents, strict=True)
    except ValueError as e:
        # Includes decoding and pydantic validation errors.
        raise EndpointConfigError(
            f'The {model.DESCRIPTION} at {path} is malformed: {e}',
        ) from None


def write_model(path: str, instance: VersionedFile) -> None:
    """Atomically write a file only the owner can access.

    The file is written as TOML if `path` ends with `.toml` and as JSON
    otherwise. See
    [`write_private_file()`][proxystore.endpoint.files.write_private_file].
    """
    if path.endswith('.toml'):
        data = dumps(instance)
    else:
        data = instance.model_dump_json(indent=2)
    write_private_file(path, data.encode())
