from __future__ import annotations

import copy
import gc
import os
import pathlib
import pickle
import sys
from typing import Generator
from typing import TypeVar

import pytest

from proxystore.connectors.file import FileConnector
from proxystore.proxy import is_resolved
from proxystore.proxy import Proxy
from proxystore.store import Store
from proxystore.store import store_registration
from proxystore.store.factory import StoreFactory
from proxystore.store.ref import _WeakRefFinalizer
from proxystore.store.ref import borrow
from proxystore.store.ref import clone
from proxystore.store.ref import into_owned
from proxystore.store.ref import mut_borrow
from proxystore.store.ref import MutableBorrowError
from proxystore.store.ref import OwnedProxy
from proxystore.store.ref import ReferenceInvalidError
from proxystore.store.ref import ReferenceNotOwnedError
from proxystore.store.ref import update

T = TypeVar('T')


@pytest.fixture()
def store(
    tmp_path: pathlib.Path,
) -> Generator[Store[FileConnector], None, None]:
    with Store(
        'stream-test-fixture',
        FileConnector(str(tmp_path)),
        cache_size=0,
    ) as store:
        with store_registration(store):
            yield store
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
        f for f in os.listdir(store.connector.store_dir) if os.path.isfile(f)
    ]
    return len(files) == 0


def test_weakref_finalizer() -> None:
    data = {'value': 0}

    class _TestObject:
        def __init__(self, d: dict[str, int]) -> None:
            self.d = d

        def inc(self) -> None:
            self.d['value'] += 1

    obj = _TestObject(data)
    finalizer = _WeakRefFinalizer(obj, 'inc')

    assert obj.d['value'] == data['value'] == 0
    finalizer()
    assert obj.d['value'] == data['value'] == 1

    del obj
    # finalizer only hold weakref so after deleting obj, finalizer
    # won't be able to invoke obj
    finalizer()
    assert data['value'] == 1


def test_owned_proxy_out_of_scope_evicts(store: Store[FileConnector]) -> None:
    def _test_in_scope() -> None:
        factory = put_in_store('value', store)
        proxy = OwnedProxy(factory)
        # The local variable proxy is one reference, and the getrefcount()
        # holds the other reference
        assert sys.getrefcount(proxy) == 2

    _test_in_scope()
    gc.collect()
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


def test_borrow_delete_owned_proxy_error(store: Store[FileConnector]) -> None:
    factory = put_in_store('value', store)
    proxy = OwnedProxy(factory)

    _borrowed = borrow(proxy)

    # We can't use `del proxy` because del only calls __del__ once the
    # reference count of proxy is 0, but borrowed has a reference to proxy
    # keeping the count at 1. In general, this is good and should prevent
    # this runtime error from happening, but we want to raise a helpful
    # message in the event we get into this position.
    with pytest.raises(RuntimeError, match='^Cannot safely delete OwnedProxy'):
        proxy.__del__()


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
    assert object.__getattribute__(proxy, '__ref_count__') == 1


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
    assert object.__getattribute__(proxy, '__ref_mut_count__') == 1


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

    del proxy.__wrapped__
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
    del proxy.__wrapped__
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
