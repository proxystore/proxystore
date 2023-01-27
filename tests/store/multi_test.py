from __future__ import annotations

import json

import pytest

from proxystore.store.multi import Policy


def test_policy_size_validation() -> None:
    policy = Policy(min_size=1, max_size=100)
    assert policy.is_valid()
    assert policy.is_valid(size=1)
    assert policy.is_valid(size=100)
    assert not policy.is_valid(size=0)
    assert not policy.is_valid(size=101)


def test_policy_subset_tags_validation() -> None:
    policy = Policy()
    assert policy.is_valid()
    assert not policy.is_valid(subset_tags=['anything'])
    policy = Policy(subset_tags=['a', 'b'])
    assert policy.is_valid(subset_tags=['a'])
    assert policy.is_valid(subset_tags=['a', 'b'])
    assert not policy.is_valid(subset_tags='other')


def test_policy_superset_tags_validation() -> None:
    policy = Policy()
    assert policy.is_valid()
    assert policy.is_valid(superset_tags='anything')
    policy = Policy(superset_tags=['a', 'b'])
    assert policy.is_valid(superset_tags=['a', 'b', 'c'])
    assert not policy.is_valid(superset_tags=['a'])
    assert not policy.is_valid(superset_tags=['c'])


@pytest.mark.parametrize(
    'policy',
    (
        Policy(priority=42),
        Policy(min_size=1, max_size=2),
        Policy(subset_tags=['a', 'b'], superset_tags=['c']),
    ),
)
def test_policy_dict_jsonable(policy: Policy) -> None:
    json.dumps(policy.as_dict())


@pytest.mark.parametrize(
    'policy',
    (
        Policy(priority=42),
        Policy(min_size=1, max_size=2),
        Policy(subset_tags=['a', 'b'], superset_tags=['c']),
    ),
)
def test_policy_dict_conversion(policy: Policy) -> None:
    assert policy == Policy(**policy.as_dict())
