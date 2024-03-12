"""Read and write TOML config files using Pydantic BaseClasses."""

from __future__ import annotations

import sys
from typing import BinaryIO
from typing import TypeVar

import tomli_w
from pydantic import BaseModel
from pydantic import VERSION

if VERSION.startswith('2'):
    pydantic_v2 = True
else:  # pragma: no cover
    pydantic_v2 = False

    import warnings

    warnings.warn(
        'Pydantic V1 compatibility is deprecated and will be removed in '
        'the future.',
        DeprecationWarning,
        stacklevel=2,
    )

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    import tomllib
else:  # pragma: <3.11 cover
    import tomli as tomllib


BaseModelT = TypeVar('BaseModelT', bound=BaseModel)


def dump(
    model: BaseModel,
    fp: BinaryIO,
    *,
    exclude_none: bool = True,
) -> None:
    """Serialize data class as a TOML formatted stream to file-like object.

    Args:
        model: Config model instance to write.
        fp: File-like bytes stream to write to.
        exclude_none: Skip writing none attributes.
    """
    if pydantic_v2:
        data_dict = model.model_dump(exclude_none=exclude_none)
    else:  # pragma: no cover
        data_dict = model.dict(exclude_none=exclude_none)
    tomli_w.dump(data_dict, fp)


def dumps(model: BaseModel, *, exclude_none: bool = True) -> str:
    """Serialize data class to a TOML formatted string.

    Args:
        model: Config model instance to write.
        exclude_none: Skip writing none attributes.

    Returns:
        TOML string of data class.
    """
    if pydantic_v2:
        data_dict = model.model_dump(exclude_none=exclude_none)
    else:  # pragma: no cover
        data_dict = model.dict(exclude_none=exclude_none)
    return tomli_w.dumps(data_dict)


def load(model: type[BaseModelT], fp: BinaryIO) -> BaseModelT:
    """Parse TOML from a binary file to a data class.

    Args:
        model: Config model type to parse TOML using.
        fp: File-like bytes stream to read in.

    Returns:
        Model initialized from TOML file.
    """
    return loads(model, fp.read().decode())


def loads(model: type[BaseModelT], data: str) -> BaseModelT:
    """Parse TOML string to data class.

    Args:
        model: Config model type to parse TOML using.
        data: TOML string to parse.

    Returns:
        Model initialized from TOML file.
    """
    data = tomllib.loads(data)
    if pydantic_v2:
        return model.model_validate(data, strict=True)
    else:  # pragma: no cover
        return model.parse_obj(data)
