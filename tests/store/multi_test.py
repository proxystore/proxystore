from __future__ import annotations

import contextlib
import json
from typing import Any
from typing import Generator

import pytest

from proxystore.proxy import is_resolved
from proxystore.store import get_store
from proxystore.store import register_store
from proxystore.store import unregister_store
from proxystore.store.base import Store
from proxystore.store.local import LocalStore
from proxystore.store.multi import MultiStore
from proxystore.store.multi import MultiStoreKey
from proxystore.store.multi import Policy
from proxystore.store.utils import get_key


@pytest.fixture(autouse=True)
def _check_no_stores_registered() -> Generator[None, None, None]:
    yield

    import proxystore.store

    if len(proxystore.store._stores) != 0:  # pragma: no cover
        raise RuntimeError(
            'This test did not unregister all of its stores! '
            f'Found: {proxystore.store._stores}.',
        )


@contextlib.contextmanager
def multi_store_from_policies(
    p1: Policy,
    p2: Policy,
) -> Generator[tuple[MultiStore, Store[Any], Store[Any]], None, None]:
    store1 = LocalStore('store1')
    store2 = LocalStore('store2')

    stores: dict[Store[Any], Policy] = {store1: p1, store2: p2}

    with MultiStore('multi', stores=stores) as store:
        assert store1 == get_store('store1')
        assert store2 == get_store('store2')
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


def test_multi_store_double_init_passes() -> None:
    stores: dict[Store[Any], Policy] = {LocalStore('local'): Policy()}
    with MultiStore('multi', stores=stores):
        with MultiStore('multi2', stores=stores):
            pass


def test_multi_init_from_str() -> None:
    store = LocalStore('local')
    register_store(store)

    with MultiStore('multi', stores={store.name: Policy()}):
        pass

    unregister_store(store.name)


def test_multi_init_from_str_missing() -> None:
    with pytest.raises(RuntimeError):
        MultiStore('multi', stores={'missing': Policy()})


def test_multi_store_basic_ops() -> None:
    stores: dict[Store[Any], Policy] = {LocalStore('local'): Policy()}
    with MultiStore('multi', stores=stores) as store:
        value = 'value'

        key = store.set('value')
        assert store.exists(key)
        assert store.get(key) == value

        store.evict(key)
        assert not store.exists(key)
        assert store.get(key) is None


def test_multi_store_custom_serializers() -> None:
    stores: dict[Store[Any], Policy] = {LocalStore('local'): Policy()}
    with MultiStore('multi', stores=stores) as store:
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
    stores: dict[Store[Any], Policy] = {
        LocalStore('local'): Policy(max_size=1),
    }

    with MultiStore('multi', stores=stores) as store:
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

        # Proxy resolve should register the multistore
        assert isinstance(get_store(multi_store.name), MultiStore)
        unregister_store(multi_store.name)


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

        # Proxy resolve should register the multistore
        assert isinstance(get_store(multi_store.name), MultiStore)
        unregister_store(multi_store.name)


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

        # Proxy resolve should register the multistore
        assert isinstance(get_store(multi_store.name), MultiStore)
        unregister_store(multi_store.name)


def test_multi_store_proxy_reregisters_stores() -> None:
    with multi_store_from_policies(
        Policy(priority=1, subset_tags=['a', 'b']),
        Policy(priority=2, superset_tags=['x', 'y']),
    ) as (multi_store, store1, store2):
        value = 'value'

        unregister_store(store1.name)
        unregister_store(store2.name)

        proxy = multi_store.proxy(value, subset_tags=['a'])
        assert proxy == value

        # Proxy resolve should register the multistore and the substores
        assert isinstance(get_store(multi_store.name), MultiStore)
        assert get_store(store1.name) is not None
        assert get_store(store2.name) is not None
        unregister_store(multi_store.name)
