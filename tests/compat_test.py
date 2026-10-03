from __future__ import annotations

import copy
import pickle
import warnings
from typing import Any

import pytest

from proxystore._compat import drop_unknown_fields
from proxystore._compat import init_kwargs
from proxystore._compat import STATE_VERSION_KEY
from proxystore.connectors.local import LocalConnector
from proxystore.connectors.multi import MultiConnector
from proxystore.connectors.multi import Policy
from proxystore.store.config import _load_connector_config
from proxystore.store.config import _load_store_config
from proxystore.store.config import ConnectorConfig
from proxystore.store.config import StoreConfig
from proxystore.store.factory import PollingStoreFactory
from proxystore.store.factory import StoreFactory
from proxystore.store.future import PollingPolicy
from proxystore.stream.events import dict_to_event
from proxystore.stream.events import EndOfStreamEvent
from proxystore.stream.events import event_to_dict
from proxystore.stream.events import EventBatch
from proxystore.stream.events import NewObjectEvent
from proxystore.warnings import VersionMismatchWarning

CONFIG = StoreConfig(
    id='store-id',
    name='test',
    connector=ConnectorConfig(kind='local', options={'option': 1}),
    cache_size=4,
)


def test_drop_unknown_fields() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        assert drop_unknown_fields('Type', {'a': 1}, ['a', 'b']) == {'a': 1}

    with pytest.warns(VersionMismatchWarning, match='Type: c, d'):
        data = drop_unknown_fields('Type', {'a': 1, 'd': 2, 'c': 3}, ['a'])
    assert data == {'a': 1}


def test_init_kwargs() -> None:
    def _func(a: int, b: int = 0) -> None: ...

    def _func_kwargs(a: int, **kwargs: Any) -> None: ...

    with pytest.warns(VersionMismatchWarning, match='_func: c'):
        assert init_kwargs(_func, {'a': 1, 'c': 2}) == {'a': 1}
    assert init_kwargs(_func_kwargs, {'a': 1, 'c': 2}) == {'a': 1, 'c': 2}


@pytest.mark.parametrize('config', (CONFIG, CONFIG.connector))
def test_config_pickle_and_copy(config: StoreConfig | ConnectorConfig) -> None:
    assert pickle.loads(pickle.dumps(config)) == config
    assert copy.deepcopy(config) == config


def test_load_store_config_unknown_and_missing_fields() -> None:
    state = CONFIG.__reduce__()[1][0]
    assert state[STATE_VERSION_KEY] == 1
    state['new_field'] = 'value'
    del state['cache_size']

    with pytest.warns(VersionMismatchWarning, match='new_field'):
        config = _load_store_config(state)
    assert config.id == CONFIG.id
    assert config.cache_size == StoreConfig.model_fields['cache_size'].default


def test_load_connector_config_unknown_fields() -> None:
    state = CONFIG.connector.__reduce__()[1][0]
    state['new_field'] = 'value'

    with pytest.warns(VersionMismatchWarning, match='new_field'):
        config = _load_connector_config(state)
    assert config == CONFIG.connector


def test_connector_from_config_unknown_options() -> None:
    config = LocalConnector().config()
    config['new_option'] = True

    with pytest.warns(VersionMismatchWarning, match='new_option'):
        connector = LocalConnector.from_config(config)
    connector.close()


def test_multi_connector_policy_unknown_fields() -> None:
    with MultiConnector({'local': (LocalConnector(), Policy())}) as connector:
        config = connector.config()
        path, options, policy = config['local']
        new_policy: dict[str, Any] = {**policy, 'new_field': 1}
        config['local'] = (path, options, new_policy)  # type: ignore[assignment]

        with pytest.warns(VersionMismatchWarning, match='new_field'):
            MultiConnector.from_config(config).close()


def test_store_factory_pickle() -> None:
    factory: StoreFactory[Any, Any] = StoreFactory(
        ('key',),
        CONFIG,
        evict=True,
    )
    new = pickle.loads(pickle.dumps(factory))
    assert new.key == factory.key
    assert new.store_config == factory.store_config
    assert new.evict
    assert new.deserializer is None


def test_store_factory_unknown_and_missing_state() -> None:
    factory: StoreFactory[Any, Any] = StoreFactory(
        ('key',),
        CONFIG,
        evict=True,
    )
    state = factory.__getstate__()
    state['new_field'] = 'value'
    del state['evict']

    new: StoreFactory[Any, Any] = StoreFactory.__new__(StoreFactory)
    with pytest.warns(VersionMismatchWarning, match='new_field'):
        new.__setstate__(state)
    assert new.key == factory.key
    assert not new.evict


def test_polling_store_factory_state() -> None:
    factory: PollingStoreFactory[Any, Any] = PollingStoreFactory(
        ('key',),
        CONFIG,
        polling=PollingPolicy(interval=2, timeout=3),
    )
    new = pickle.loads(pickle.dumps(factory))
    assert new.polling == factory.polling

    state = factory.__getstate__()
    del state['polling_interval']
    new = PollingStoreFactory.__new__(PollingStoreFactory)
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        new.__setstate__(state)
    assert new.polling == PollingPolicy(interval=1, timeout=3)


def test_event_unknown_fields() -> None:
    batch = EventBatch(
        'topic',
        [NewObjectEvent('topic', 'value', {}), EndOfStreamEvent('topic')],
    )
    data = event_to_dict(batch)
    data['new_field'] = 1
    data['events'][0]['new_field'] = 1

    with pytest.warns(VersionMismatchWarning, match='new_field'):
        assert dict_to_event(data) == batch
