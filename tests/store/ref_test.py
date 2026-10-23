from __future__ import annotations

import copy
import gc
import logging
import os
import pathlib
import pickle
import weakref
from collections.abc import Callable
from collections.abc import Generator
from typing import Any
from typing import TypeVar
from unittest import mock

import pytest

from proxystore.connectors.file import FileConnector
from proxystore.connectors.local import LocalConnector
from proxystore.proxy import is_resolved
from proxystore.proxy import Proxy
from proxystore.store import ref
from proxystore.store import Store
from proxystore.store._evictions import evictions
from proxystore.store.exceptions import ProxyResolveMissingKeyError
from proxystore.store.exceptions import ProxyStoreFactoryError
from proxystore.store.factory import StoreFactory
from proxystore.store.ref import borrow
from proxystore.store.ref import clone
from proxystore.store.ref import into_owned
from proxystore.store.ref import mut_borrow
from proxystore.store.ref import MutableBorrowError
from proxystore.store.ref import OwnedProxy
from proxystore.store.ref import ReferenceBorrowedError
from proxystore.store.ref import ReferenceInvalidError
from proxystore.store.ref import ReferenceNotOwnedError
from proxystore.store.ref import update

T = TypeVar('T')


@pytest.fixture
def store(
    tmp_path: pathlib.Path,
) -> Generator[Store[FileConnector], None, None]:
    with Store(FileConnector(tmp_path), cache_size_bytes=0) as store:
        yield store
        # Owned proxies freed by the test evict their objects in the
        # background.
        evictions.wait()
        if not store_is_empty(store):  # pragma: no cover
            raise RuntimeError('Test left objects in the store.')


def put_in_store(
    obj: T,
    store: Store[FileConnector],
) -> StoreFactory[FileConnector, T]:
    key = store.put(obj)
    return StoreFactory(key, store.config(), evict=False)


def store_is_empty(store: Store[FileConnector]) -> bool:
    files = [
        f
        for f in os.listdir(store.connector.store_dir)
        if os.path.isfile(os.path.join(store.connector.store_dir, f))
    ]
    return len(files) == 0


@pytest.fixture
def live_refs() -> Generator[
    weakref.WeakValueDictionary[int, ref.BaseRefProxy[object]],
    None,
    None,
]:
    # Use a separate registry so finalizing at exit in a test only
    # touches the proxies made by that test.
    refs: weakref.WeakValueDictionary[int, ref.BaseRefProxy[object]] = (
        weakref.WeakValueDictionary()
    )
    with mock.patch.object(ref, '_live_refs', refs):
        yield refs


def test_atexit_callback_registered_once(
    store: Store[FileConnector],
    live_refs: weakref.WeakValueDictionary[int, ref.BaseRefProxy[object]],
) -> None:
    with (
        mock.patch.object(ref, '_atexit_registered', False),
        mock.patch('proxystore.store.ref.atexit') as mock_atexit,
    ):
        proxy = OwnedProxy(put_in_store('value', store))
        borrowed = borrow(proxy)
        other = OwnedProxy(put_in_store('value', store))

    mock_atexit.register.assert_called_once_with(ref._finalize_live_refs)
    assert len(live_refs) == 3

    del borrowed, proxy, other
    gc.collect()
    assert len(live_refs) == 0


def test_finalize_live_refs(
    store: Store[FileConnector],
    live_refs: weakref.WeakValueDictionary[int, ref.BaseRefProxy[object]],
) -> None:
    proxy1 = OwnedProxy(put_in_store('value', store))
    borrowed = borrow(proxy1)
    proxy2 = OwnedProxy(put_in_store('value', store))
    mut_borrowed = mut_borrow(proxy2)

    # Borrowed references are made after their owners so they should be
    # released first and the owners should not raise.
    ref._finalize_live_refs()

    assert store_is_empty(store)
    for proxy in (proxy1, borrowed, proxy2, mut_borrowed):
        assert not object.__getattribute__(proxy, '__proxy_valid__')
    assert object.__getattribute__(borrowed, '__proxy_owner__') is None
    assert object.__getattribute__(mut_borrowed, '__proxy_owner__') is None


def test_finalize_live_refs_continues_after_error(
    store: Store[FileConnector],
    live_refs: weakref.WeakValueDictionary[int, ref.BaseRefProxy[object]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    other_factory = put_in_store('value', store)
    other = OwnedProxy(other_factory)
    proxy = OwnedProxy(put_in_store('value', store))
    evict_if_dropped = ref._evict_if_dropped

    def _evict(owner: OwnedProxy[Any]) -> None:
        if owner is proxy:
            raise RuntimeError('failed')
        evict_if_dropped(owner)

    with (
        caplog.at_level(logging.ERROR, logger='proxystore.store.ref'),
        mock.patch.object(ref, '_evict_if_dropped', side_effect=_evict),
    ):
        ref._finalize_live_refs()

    # The error finalizing proxy is logged, and the older proxy is still
    # finalized.
    assert 'Failed to finalize OwnedProxy at exit' in caplog.text
    assert not object.__getattribute__(other, '__proxy_valid__')
    assert not store.exists(other_factory.key)

    ref._evict_if_dropped(proxy)
    evictions.wait()


def test_owned_proxy_out_of_scope_evicts(store: Store[FileConnector]) -> None:
    def _test_in_scope() -> None:
        factory = put_in_store('value', store)
        _proxy = OwnedProxy(factory)

    _test_in_scope()
    gc.collect()
    evictions.wait()
    assert store_is_empty(store)


def test_borrow_behaves_as_value(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)

    borrowed1 = borrow(proxy)
    assert borrowed1 == 'value'

    borrowed2 = borrow(proxy)
    assert borrowed2 == 'value'


def test_mut_borrow_behaves_as_value(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)

    borrowed = mut_borrow(proxy)
    assert borrowed == 'value'


def test_borrow_does_not_resolve(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)

    assert not is_resolved(proxy)
    borrowed = borrow(proxy)
    assert not is_resolved(proxy)
    assert not is_resolved(borrowed)
    assert borrowed == 'value'


def test_mut_borrow_does_not_resolve(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)

    assert not is_resolved(proxy)
    borrowed = mut_borrow(proxy)
    assert not is_resolved(proxy)
    assert not is_resolved(borrowed)
    assert borrowed == 'value'


@pytest.mark.parametrize('borrow_func', (borrow, mut_borrow))
def test_owned_proxy_freed_while_borrowed(
    borrow_func: Callable[[OwnedProxy[str]], Proxy[str]],
    store: Store[FileConnector],
) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)
    borrowed = borrow_func(proxy)

    # `del proxy` would not call __del__ because borrowed references proxy,
    # so call it directly like the garbage collector does for a cycle.
    proxy.__del__()
    evictions.wait()
    assert store.exists(factory.key)
    assert borrowed == 'value'

    # The last borrow evicts the object once it is freed.
    del borrowed
    evictions.wait()
    assert not store.exists(factory.key)


class _Holder:
    owner: OwnedProxy[str]
    borrowed: Proxy[str]
    cycle: _Holder


def test_owned_proxy_in_cycle_with_borrow(store: Store[FileConnector]) -> None:
    keys = []
    for _ in range(10):
        factory = put_in_store('value', store)
        keys.append(factory.key)
        holder = _Holder()
        holder.owner = OwnedProxy(factory)
        holder.borrowed = borrow(holder.owner)
        holder.cycle = holder
    del holder

    # The garbage collector frees the owner and the borrow in any order.
    gc.collect()
    evictions.wait()
    assert not any(store.exists(key) for key in keys)


def test_clone(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)

    cloned = clone(proxy)
    assert isinstance(cloned, OwnedProxy)
    del proxy
    assert cloned == 'value'


def test_update(store: Store[FileConnector]) -> None:
    factory = put_in_store([1, 2, 3], store)
    proxy = OwnedProxy(factory)

    proxy.append(4)
    update(proxy)

    assert store.get(factory.key) == [1, 2, 3, 4]


def test_update_unresolved(store: Store[FileConnector]) -> None:
    factory = put_in_store([1, 2, 3], store)
    proxy = OwnedProxy(factory)
    update(proxy)
    assert proxy == [1, 2, 3]


def test_pickle_owned_proxy(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)

    proxy_pkl = pickle.dumps(proxy)

    with pytest.raises(ReferenceInvalidError):
        # Old proxy should be invalid
        assert proxy == 'value'

    new_proxy = pickle.loads(proxy_pkl)
    assert new_proxy == 'value'


def test_pickle_owned_proxy_twice(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)

    new_proxy = pickle.loads(pickle.dumps(proxy))
    with pytest.raises(ReferenceInvalidError, match='already pickled'):
        pickle.dumps(proxy)
    assert new_proxy == 'value'


@pytest.mark.parametrize('borrow_func', (borrow, mut_borrow))
def test_pickle_borrowed_owned_proxy(
    store: Store[FileConnector],
    borrow_func: Callable[[OwnedProxy[str]], Any],
) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)
    borrowed = borrow_func(proxy)

    with pytest.raises(ReferenceBorrowedError):
        pickle.dumps(proxy)
    # The proxy keeps ownership so it is still valid.
    assert proxy == 'value'

    del borrowed
    new_proxy = pickle.loads(pickle.dumps(proxy))
    assert new_proxy == 'value'


def test_pickle_ref_proxy(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)

    borrowed = borrow(proxy)
    borrowed_pkl = pickle.dumps(borrowed)

    with pytest.raises(ReferenceInvalidError):
        # Only proxy should be invalid
        assert borrowed == 'value'

    new_borrowed = pickle.loads(borrowed_pkl)
    assert new_borrowed == 'value'

    del new_borrowed
    # new_borrowed, because it was pickled and unpickled, does not have a
    # reference to is owned proxy so deleting new_borrowed will not
    # decrement the ref count on proxy.
    assert object.__getattribute__(proxy, '__proxy_ref_count__') == 1


def test_pickle_ref_mut_proxy(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)

    borrowed = mut_borrow(proxy)
    borrowed_pkl = pickle.dumps(borrowed)

    with pytest.raises(ReferenceInvalidError):
        # Only proxy should be invalid
        assert borrowed == 'value'

    new_borrowed = pickle.loads(borrowed_pkl)
    assert new_borrowed == 'value'

    del new_borrowed
    # new_borrowed, because it was pickled and unpickled, does not have a
    # reference to is owned proxy so deleting new_borrowed will not
    # decrement the ref count on proxy.
    assert object.__getattribute__(proxy, '__proxy_ref_mut_count__') == 1


def test_copy_not_implemented_error(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)

    message = '^Copy is not implemented'

    with pytest.raises(NotImplementedError, match=message):
        copy.copy(proxy)

    borrowed = borrow(proxy)
    with pytest.raises(NotImplementedError, match=message):
        copy.copy(borrowed)
    del borrowed

    borrowed = mut_borrow(proxy)
    with pytest.raises(NotImplementedError, match=message):
        copy.copy(borrowed)
    del borrowed


def test_deepcopy_not_implemented_error(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)

    message = '^Deep copy is not implemented'

    with pytest.raises(NotImplementedError, match=message):
        copy.deepcopy(proxy)

    borrowed = borrow(proxy)
    with pytest.raises(NotImplementedError, match=message):
        copy.deepcopy(borrowed)
    del borrowed

    borrowed = mut_borrow(proxy)
    with pytest.raises(NotImplementedError, match=message):
        copy.deepcopy(borrowed)
    del borrowed


def test_borrow_unowned_error(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)

    with pytest.raises(ReferenceNotOwnedError):
        borrow(borrow(proxy))


def test_mut_borrow_unowned_error(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)

    with pytest.raises(ReferenceNotOwnedError):
        mut_borrow(borrow(proxy))


def test_borrow_already_mutably_error(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)
    _borrowed = mut_borrow(proxy)

    with pytest.raises(MutableBorrowError):
        borrow(proxy)


def test_mut_borrow_already_immutably_error(
    store: Store[FileConnector],
) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)
    _borrowed = borrow(proxy)

    with pytest.raises(MutableBorrowError):
        mut_borrow(proxy)


def test_mut_borrow_already_mutably_error(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)
    _borrowed = mut_borrow(proxy)

    with pytest.raises(MutableBorrowError):
        mut_borrow(proxy)


@pytest.mark.parametrize('func', (borrow, mut_borrow, clone))
def test_invalid_owner_error(
    func: Callable[[OwnedProxy[str]], Proxy[str]],
    store: Store[FileConnector],
) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)
    new_proxy = pickle.loads(pickle.dumps(proxy))

    # Pickling moved ownership to new_proxy, which may evict the object.
    with pytest.raises(ReferenceInvalidError, match='already pickled'):
        func(proxy)

    del new_proxy


def test_clone_unowned_error(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)

    with pytest.raises(ReferenceNotOwnedError):
        clone(borrow(proxy))


def test_update_unowned_error(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)

    with pytest.raises(ReferenceNotOwnedError):
        update(borrow(proxy))


def test_update_already_borrowed(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)
    _borrowed = borrow(proxy)

    with pytest.raises(MutableBorrowError):
        update(proxy)


def test_into_owned(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = Proxy(factory)
    assert not is_resolved(proxy)
    owned_proxy = into_owned(proxy)
    assert not is_resolved(proxy)
    assert isinstance(owned_proxy, OwnedProxy)


def test_into_owned_value_error(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)
    with pytest.raises(ValueError, match='Only a base proxy can be'):
        into_owned(proxy)


def test_into_owned_factory_error(store: Store[FileConnector]) -> None:
    def factory() -> str:  # pragma: no cover
        return 'value'

    proxy = Proxy(factory)
    with pytest.raises(
        ProxyStoreFactoryError,
        match='The proxy must contain a factory with type StoreFactory',
    ):
        into_owned(proxy)


@pytest.mark.parametrize('populate_target', (True, False))
def test_borrow_populate(
    populate_target: bool,
    store: Store[FileConnector],
) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)
    assert isinstance(proxy, str)

    borrowed = borrow(proxy, populate_target=populate_target)
    assert is_resolved(borrowed) == populate_target

    del proxy.__proxy_wrapped__
    borrowed = borrow(proxy, populate_target=populate_target)
    assert not is_resolved(borrowed)


@pytest.mark.parametrize('populate_target', (True, False))
def test_mut_borrow_populate(
    populate_target: bool,
    store: Store[FileConnector],
) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)
    assert isinstance(proxy, str)

    borrowed = mut_borrow(proxy, populate_target=populate_target)
    assert is_resolved(borrowed) == populate_target

    del borrowed
    del proxy.__proxy_wrapped__
    borrowed = mut_borrow(proxy, populate_target=populate_target)
    assert not is_resolved(borrowed)


@pytest.mark.parametrize('populate_target', (True, False))
def test_into_owned_populate(
    populate_target: bool,
    store: Store[FileConnector],
) -> None:
    factory = put_in_store('value', store)
    proxy = Proxy(factory)
    assert isinstance(proxy, str)

    owned = into_owned(proxy, populate_target=populate_target)
    assert is_resolved(owned) == populate_target


def test_del_invalid_owned_proxy(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)

    proxy_pkl = pickle.dumps(proxy)
    new_proxy = pickle.loads(proxy_pkl)

    # Should do nothing because old proxy was made invalid
    del proxy

    assert new_proxy == 'value'


def test_clone_copies_stored_object(store: Store[FileConnector]) -> None:
    value = [1, 2, 3]
    factory = put_in_store(value, store)
    proxy = OwnedProxy(factory, target=value, cache_defaults=True)
    assert is_resolved(proxy)

    cloned = clone(proxy)
    # The clone resolves its own copy from the store.
    assert not is_resolved(cloned)
    assert isinstance(cloned, list)
    assert not is_resolved(cloned)
    assert cloned.__proxy_factory__.key != factory.key

    # A change to the local target which was not written to the store
    # with update() is not in the clone.
    proxy.append(4)
    assert cloned == [1, 2, 3]
    cloned.append(5)
    assert proxy == [1, 2, 3, 4]

    # The copy does not depend on the original object.
    del proxy
    gc.collect()
    evictions.wait()
    assert not store.exists(factory.key)
    assert store.get(cloned.__proxy_factory__.key) == [1, 2, 3]


def test_clone_missing_object(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)
    store.evict(factory.key)

    with pytest.raises(ProxyResolveMissingKeyError):
        clone(proxy)


def test_clone_mutably_borrowed(store: Store[FileConnector]) -> None:
    factory = put_in_store([1, 2, 3], store)
    proxy = OwnedProxy(factory)
    borrowed = mut_borrow(proxy)

    with pytest.raises(MutableBorrowError, match='mutable'):
        clone(proxy)

    del borrowed
    assert clone(proxy) == [1, 2, 3]


def test_clone_records_metrics() -> None:
    with Store(LocalConnector(), metrics=True) as store:
        proxy = store.owned_proxy('value')
        cloned = clone(proxy)
        new_key = cloned.__proxy_factory__.key

        assert store.metrics is not None
        source_metrics = store.metrics.get_metrics(proxy.__proxy_factory__.key)
        assert source_metrics is not None
        assert 'store.copy.get' in source_metrics.times
        metrics = store.metrics.get_metrics(new_key)
        assert metrics is not None
        assert {'store.copy', 'store.copy.put'} <= set(metrics.times)
        assert metrics.attributes['store.copy.object_size'] > 0
        # Owned proxies evict their objects so they are deleted before the
        # store is closed.
        del proxy, cloned


def test_owned_proxy_cache_defaults(store: Store[FileConnector]) -> None:
    value = 'value'
    factory = put_in_store(value, store)
    proxy = OwnedProxy(factory, cache_defaults=True, target=value)
    del proxy.__proxy_wrapped__

    assert not is_resolved(proxy)
    assert isinstance(proxy, str)
    assert not is_resolved(proxy)

    assert not is_resolved(proxy)
    assert hash(proxy) == hash(value)
    assert not is_resolved(proxy)
