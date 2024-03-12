from __future__ import annotations

import asyncio
import os
import pathlib
from concurrent.futures import Future
from concurrent.futures import ProcessPoolExecutor
from typing import Any
from typing import Callable
from typing import Generator

import pytest

from proxystore.connectors.file import FileConnector
from proxystore.proxy import is_resolved
from proxystore.store import Store
from proxystore.store import store_registration
from proxystore.store.ref import borrow
from proxystore.store.ref import mut_borrow
from proxystore.store.ref import ReferenceInvalidError
from proxystore.store.scopes import _make_out_of_scope_callback
from proxystore.store.scopes import FutureWithCallback
from proxystore.store.scopes import mark_refs_out_of_scope
from proxystore.store.scopes import submit


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


def store_is_empty(store: Store[FileConnector]) -> bool:
    files = [
        f for f in os.listdir(store.connector.store_dir) if os.path.isfile(f)
    ]
    return len(files) == 0


@pytest.mark.asyncio()
async def test_future_protocol() -> None:
    # Test a few future implementations to make sure they match the
    # FutureWithCallback protocol.
    assert isinstance(Future(), FutureWithCallback)

    loop = asyncio.get_running_loop()
    assert isinstance(loop.create_future(), FutureWithCallback)

    class _TestFuture:
        def set_result(self, *args: Any) -> None: ...

        def add_done_callback(self, fn: Callable[[Any], None]) -> None: ...

    assert isinstance(_TestFuture(), FutureWithCallback)


def test_mark_refs_out_of_scope(store: Store[FileConnector]) -> None:
    proxy1 = store.owned_proxy('value1')
    proxy2 = store.owned_proxy('value2')

    borrowed1 = borrow(proxy1)
    borrowed2 = borrow(proxy1)
    borrowed3 = mut_borrow(proxy2)

    mark_refs_out_of_scope(borrowed1, borrowed2, borrowed3)

    for borrowed in (borrowed1, borrowed2, borrowed3):
        with pytest.raises(ReferenceInvalidError):
            assert isinstance(borrowed, str)

    # Making mutable borrows would have failed if the prior borrows still
    # existed.
    mut_borrow(proxy1)
    mut_borrow(proxy2)


def test_mark_refs_out_of_scope_duplicates(
    store: Store[FileConnector],
) -> None:
    proxy = store.owned_proxy('value')

    borrowed = borrow(proxy)

    mark_refs_out_of_scope(borrowed, borrowed)

    with pytest.raises(ReferenceInvalidError):
        assert isinstance(borrowed, str)


def test_mark_refs_out_of_scope_no_owner(store: Store[FileConnector]) -> None:
    proxy = store.owned_proxy('value')
    borrowed = borrow(proxy)
    object.__setattr__(borrowed, '__owner__', None)

    with pytest.raises(RuntimeError, match='no reference to its owner'):
        mark_refs_out_of_scope(borrowed)

    # Restore owner so cleanup can be done correctly
    object.__setattr__(borrowed, '__owner__', proxy)


def test_make_out_of_scope_callback(store: Store[FileConnector]) -> None:
    proxy = store.owned_proxy('value1')
    borrowed = borrow(proxy)

    callback = _make_out_of_scope_callback([borrowed])
    callback(Future())

    with pytest.raises(ReferenceInvalidError):
        assert isinstance(borrowed, str)


def test_submit_registration(store: Store[FileConnector]) -> None:
    proxy1 = store.owned_proxy(2)
    proxy2 = store.owned_proxy(3)
    proxy3 = store.owned_proxy(7)

    borrow1 = borrow(proxy1)
    borrow2 = borrow(proxy2)
    borrow3 = borrow(proxy3)

    def _test_func(arg1: int, *, kwarg1: int = 0) -> Future[int]:
        fut: Future[int] = Future()
        fut.set_result(arg1 + kwarg1)
        return fut

    fut = submit(
        _test_func,
        args=(borrow1,),
        kwargs={'kwarg1': borrow2},
        register_custom_refs=[borrow3],
    )

    assert fut.result() == 5

    for p in (borrow1, borrow2, borrow3):
        with pytest.raises(ReferenceInvalidError):
            assert isinstance(p, int)


def test_submit_does_not_resolve(store: Store[FileConnector]) -> None:
    proxy = store.owned_proxy('value')
    borrowed = borrow(proxy)

    def _test_func(*args, **kwargs) -> Future[str]:
        fut: Future[str] = Future()
        fut.set_result('result')
        return fut

    assert not is_resolved(proxy)
    assert not is_resolved(borrowed)

    fut = submit(_test_func, args=(proxy, borrowed))

    assert not is_resolved(proxy)
    assert not is_resolved(borrowed)

    assert fut.result() == 'result'


def test_submit_with_multiprocessing(store: Store[FileConnector]) -> None:
    proxy = store.owned_proxy('value')

    with ProcessPoolExecutor(max_workers=1) as pool:
        borrowed = borrow(proxy)
        fut: Future[bool] = submit(
            pool.submit,
            args=(isinstance, borrowed, str),
        )

        # future should return True (the result of isinstance(borrowed, str))
        # and borrowed should be marked invalid
        assert fut.result()
        with pytest.raises(ReferenceInvalidError):
            assert isinstance(borrowed, str)
