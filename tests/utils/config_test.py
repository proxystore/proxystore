from __future__ import annotations

import pathlib
from typing import List
from typing import Optional

from pydantic import BaseModel

from proxystore.utils.config import dump
from proxystore.utils.config import dumps
from proxystore.utils.config import load
from proxystore.utils.config import loads


class _Subsection(BaseModel):
    field1: float
    field2: str


class _Section(BaseModel):
    subsection: _Subsection
    values: List[int]  # noqa: UP006


class _Config(BaseModel):
    option1: bool
    option2: bool
    section: _Section


TEST_CONFIG = _Config(
    option1=True,
    option2=False,
    section=_Section(
        subsection=_Subsection(field1=42.0, field2='test'),
        values=[1, 2, 3],
    ),
)
TEST_CONFIG_REPR = """\
option1 = true
option2 = false

[section]
values = [
    1,
    2,
    3,
]

[section.subsection]
field1 = 42.0
field2 = "test"
"""


def test_dump(tmp_path: pathlib.Path) -> None:
    filepath = tmp_path / 'test.toml'

    with open(filepath, 'wb') as f:
        dump(TEST_CONFIG, f)

    with open(filepath) as f:
        assert f.read() == TEST_CONFIG_REPR


def test_dump_drops_none_values(tmp_path: pathlib.Path) -> None:
    filepath = tmp_path / 'test.toml'

    class _Config(BaseModel):
        field1: Optional[str] = None  # noqa: UP007
        field2: str = 'abc'

    with open(filepath, 'wb') as fw:
        dump(_Config(), fw)

    with open(filepath) as fr:
        data = fr.read()

    assert 'field1' not in data
    assert 'field2' in data


def test_dumps() -> None:
    assert dumps(TEST_CONFIG) == TEST_CONFIG_REPR


def test_load(tmp_path: pathlib.Path) -> None:
    filepath = tmp_path / 'test.toml'

    with open(filepath, 'w') as f:
        f.write(TEST_CONFIG_REPR)

    with open(filepath, 'rb') as f:
        assert load(_Config, f) == TEST_CONFIG


def test_loads() -> None:
    assert loads(_Config, TEST_CONFIG_REPR) == TEST_CONFIG
