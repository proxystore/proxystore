from __future__ import annotations

import pickle

import pytest

from proxystore.connectors.local import LocalConnector
from proxystore.store import get_or_create_store
from proxystore.store import get_store
from proxystore.store import register_store
from proxystore.store import Store
from proxystore.store import store_registration
from proxystore.store import unregister_store
from proxystore.store.exceptions import StoreError
from proxystore.store.exceptions import StoreExistsError


def test_register_store_is_noop() -> None:
    with Store(LocalConnector()) as store:
        with pytest.warns(DeprecationWarning, match='register_store'):
            register_store(store, exist_ok=True)
        assert get_or_create_store(store.config()) is store


def test_unregister_store_is_noop() -> None:
    with Store(LocalConnector()) as store:
        with pytest.warns(DeprecationWarning, match='unregister_store'):
            unregister_store(store)
        assert get_or_create_store(store.config()) is store


def test_store_registration_is_noop() -> None:
    with (
        Store(LocalConnector()) as store,
        pytest.warns(DeprecationWarning, match='store_registration'),
        store_registration(store, exist_ok=True),
    ):
        assert get_or_create_store(store.config()) is store


def test_get_or_create_store_ignores_register() -> None:
    with Store(LocalConnector()) as store:
        config = pickle.loads(pickle.dumps(store.config()))
        with pytest.warns(DeprecationWarning, match='register argument'):
            new_store = get_or_create_store(config, register=True)
        assert new_store is store


def test_store_exists_error() -> None:
    assert issubclass(StoreExistsError, StoreError)


def test_get_store_by_name_error() -> None:
    with pytest.raises(TypeError, match='not a store name'):
        get_store('my-store')  # type: ignore[arg-type]


def test_shims_not_public() -> None:
    import proxystore.store
    import proxystore.store.exceptions

    for name in (
        'register_store',
        'store_registration',
        'unregister_store',
    ):
        assert name not in proxystore.store.__all__

    with pytest.raises(AttributeError, match='no attribute'):
        _ = proxystore.store.exceptions.NotAnError
