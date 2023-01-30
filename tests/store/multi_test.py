from __future__ import annotations

import contextlib
import json
from typing import Any
from typing import cast
from typing import Generator

import pytest

from proxystore.proxy import is_resolved
from proxystore.store import get_store
from proxystore.store.base import Store
from proxystore.store.exceptions import StoreExistsError
from proxystore.store.local import LocalStore
from proxystore.store.multi import MultiStore
from proxystore.store.multi import MultiStoreKey
from proxystore.store.multi import Policy
from proxystore.store.multi import StorePolicyArgs
from proxystore.store.utils import get_key


@pytest.fixture
def simple_store() -> StorePolicyArgs:
    return StorePolicyArgs(
        name='local',
        kind=LocalStore,
        kwargs={},
        policy=Policy(),
    )


@contextlib.contextmanager
def multi_store_from_policies(
    p1: Policy,
    p2: Policy,
) -> Generator[tuple[MultiStore, Store[Any], Store[Any]], None, None]:
    store_args_1 = StorePolicyArgs(
        name='store1',
        kind=LocalStore,
        kwargs={},
        policy=p1,
    )
    store_args_2 = StorePolicyArgs(
        name='store2',
        kind=LocalStore,
        kwargs={},
        policy=p2,
    )

    with MultiStore('multi', stores=[store_args_1, store_args_2]) as store:
        # Store.__enter__(...) -> Store[KeyT] causes Mypy to be unable to
        # resolve the store here is specifically a MultiStore. In mypy 1.0
        # we should be able to change the return type to Self
        store = cast(MultiStore, store)
        store1 = get_store('store1')
        store2 = get_store('store2')
        assert isinstance(store1, LocalStore)
        assert isinstance(store2, LocalStore)
        yield (store, store1, store2)


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


def test_multi_store_double_init_fails(simple_store: StorePolicyArgs) -> None:
    # First init succeeds
    with MultiStore('multi', stores=[simple_store]):
        # Second init fails
        with pytest.raises(StoreExistsError):
            MultiStore('multi2', stores=[simple_store])


def test_multi_store_basic_ops(simple_store: StorePolicyArgs) -> None:
    with MultiStore('multi', stores=[simple_store]) as store:
        value = 'value'

        key = store.set('value')
        assert store.exists(key)
        assert store.get(key) == value

        store.evict(key)
        assert not store.exists(key)
        assert store.get(key) is None


def test_multi_store_custom_serializers(simple_store: StorePolicyArgs) -> None:
    with MultiStore('multi', stores=[simple_store]) as store:
        value = 'value'

        key = store.set(value, serializer=str.encode)
        assert store.get(key, deserializer=bytes.decode) == value

        with pytest.raises(TypeError):
            store.set(value, serializer=lambda x: x)


def test_multi_store_priority() -> None:
    with multi_store_from_policies(
        Policy(priority=1),
        Policy(priority=2),
    ) as (multi_store, store1, store2):
        value = 'value'
        key = multi_store.set(value)
        assert not store1.exists(key.store_key)
        assert store2.exists(key.store_key)


def test_multi_store_policy_size() -> None:
    with multi_store_from_policies(
        Policy(max_size=1),
        Policy(min_size=2),
    ) as (multi_store, store1, store2):
        value = 'value'
        key = multi_store.set(value)
        assert not store1.exists(key.store_key)
        assert store2.exists(key.store_key)


def test_multi_store_policy_tags() -> None:
    with multi_store_from_policies(
        Policy(priority=1, subset_tags=['a', 'b']),
        Policy(priority=2, superset_tags=['x', 'y']),
    ) as (multi_store, store1, store2):
        value = 'value'

        key = multi_store.set(value, subset_tags=['a'])
        assert store1.exists(key.store_key)
        assert not store2.exists(key.store_key)

        key = multi_store.set(value, superset_tags=['x', 'y', 'z'])
        assert not store1.exists(key.store_key)
        assert store2.exists(key.store_key)


def test_multi_store_policy_no_valid() -> None:
    store_args_1 = StorePolicyArgs(
        name='store1',
        kind=LocalStore,
        kwargs={},
        policy=Policy(max_size=1),
    )

    with MultiStore('multi', stores=[store_args_1]) as store:
        with pytest.raises(ValueError, match='policy'):
            store.set('value')


def test_multi_store_proxy_method() -> None:
    with multi_store_from_policies(
        Policy(priority=1, subset_tags=['a', 'b']),
        Policy(priority=2, superset_tags=['x', 'y']),
    ) as (multi_store, store1, store2):
        value = 'value'

        proxy = multi_store.proxy(value, subset_tags=['a'])
        key = get_key(proxy)
        assert isinstance(key, MultiStoreKey)
        assert store1.exists(key.store_key)
        assert not store2.exists(key.store_key)

        proxy = multi_store.proxy(value, superset_tags=['x', 'y', 'z'])
        key = get_key(proxy)
        assert isinstance(key, MultiStoreKey)
        assert not store1.exists(key.store_key)
        assert store2.exists(key.store_key)

        assert proxy == value


def test_multi_store_proxy_batch() -> None:
    with multi_store_from_policies(
        Policy(priority=1),
        Policy(priority=2),
    ) as (multi_store, store1, store2):
        values = ['value1', 'value2', 'value3']

        proxies = multi_store.proxy_batch(values)
        keys = [get_key(p) for p in proxies]

        for key, proxy, value in zip(keys, proxies, values):
            assert isinstance(key, MultiStoreKey)
            assert not store1.exists(key.store_key)
            assert store2.exists(key.store_key)

            assert proxy == value


def test_multi_store_proxy_locker() -> None:
    with multi_store_from_policies(
        Policy(priority=1, subset_tags=['a', 'b']),
        Policy(priority=2, superset_tags=['x', 'y']),
    ) as (multi_store, store1, store2):
        value = 'value'

        locked_proxy = multi_store.locked_proxy(value, subset_tags=['a'])
        assert locked_proxy != value

        proxy = locked_proxy.unlock()
        assert not is_resolved(proxy)
        key = get_key(proxy)
        assert isinstance(key, MultiStoreKey)
        assert store1.exists(key.store_key)
        assert not store2.exists(key.store_key)
        assert proxy == value
