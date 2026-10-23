from __future__ import annotations

import contextlib
import json
import logging
from collections.abc import Generator
from typing import Any
from unittest import mock

import pytest

from proxystore.connectors.local import LocalConnector
from proxystore.connectors.multi import MultiConnector
from proxystore.connectors.multi import MultiConnectorError
from proxystore.connectors.multi import MultiKey
from proxystore.connectors.multi import Policy
from proxystore.connectors.protocols import Connector
from proxystore.store.base import Store
from proxystore.store.utils import get_key


@contextlib.contextmanager
def multi_connector_from_policies(
    p1: Policy,
    p2: Policy,
) -> Generator[
    tuple[MultiConnector, LocalConnector, LocalConnector],
    None,
    None,
]:
    connector1 = LocalConnector(include_data_in_config=True)
    connector2 = LocalConnector(include_data_in_config=True)

    connectors: dict[str, tuple[Connector[Any], Policy]] = {
        'c1': (connector1, p1),
        'c2': (connector2, p2),
    }

    connector = MultiConnector(connectors)
    yield (connector, connector1, connector2)
    connector.close()


def test_policy_host_validation() -> None:
    # Default policy ignores host
    assert Policy().is_valid()
    assert Policy().is_valid_on_host()

    with mock.patch('proxystore.utils.hostname') as mock_hostname:
        mock_hostname.return_value = 'testhost'
        policy = Policy(host_pattern='(testhost|otherhost)')
        assert policy.is_valid()
        assert policy.is_valid_on_host()
        mock_hostname.return_value = 'thirdhost'
        assert not policy.is_valid()
        assert not policy.is_valid_on_host()
        policy = Policy(host_pattern=['(texthost|otherhost)', 'thirdhost'])
        assert policy.is_valid()
        assert policy.is_valid_on_host()


def test_policy_size_validation() -> None:
    policy = Policy(min_size_bytes=1, max_size_bytes=100)
    assert policy.is_valid()
    assert policy.is_valid(size_bytes=1)
    assert policy.is_valid(size_bytes=100)
    assert not policy.is_valid(size_bytes=0)
    assert not policy.is_valid(size_bytes=101)


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
        Policy(min_size_bytes=1, max_size_bytes=2),
        Policy(subset_tags=['a', 'b'], superset_tags=['c']),
    ),
)
def test_policy_dict_jsonable(policy: Policy) -> None:
    json.dumps(policy.as_dict())


@pytest.mark.parametrize(
    'policy',
    (
        Policy(priority=42),
        Policy(min_size_bytes=1, max_size_bytes=2),
        Policy(subset_tags=['a', 'b'], superset_tags=['c']),
    ),
)
def test_policy_dict_conversion(policy: Policy) -> None:
    assert policy == Policy(**policy.as_dict())


def test_multi_connector_priority() -> None:
    with multi_connector_from_policies(
        Policy(priority=1),
        Policy(priority=2),
    ) as (multi_connector, connector1, connector2):
        value = b'value'
        key = multi_connector.put(value)
        assert not connector1.exists(key.connector_key)
        assert connector2.exists(key.connector_key)


def test_multi_connector_policy_size() -> None:
    with multi_connector_from_policies(
        Policy(max_size_bytes=1),
        Policy(min_size_bytes=2),
    ) as (multi_connector, connector1, connector2):
        value = b'value'
        key = multi_connector.put(value)
        assert not connector1.exists(key.connector_key)
        assert connector2.exists(key.connector_key)


def test_multi_connector_policy_tags() -> None:
    with multi_connector_from_policies(
        Policy(priority=1, subset_tags=['a', 'b']),
        Policy(priority=2, superset_tags=['x', 'y']),
    ) as (multi_connector, connector1, connector2):
        value = b'value'

        key = multi_connector.put(value, subset_tags=['a'])
        assert connector1.exists(key.connector_key)
        assert not connector2.exists(key.connector_key)

        key = multi_connector.put(value, superset_tags=['x', 'y', 'z'])
        assert not connector1.exists(key.connector_key)
        assert connector2.exists(key.connector_key)


def test_multi_connector_iterator_tags() -> None:
    with multi_connector_from_policies(
        Policy(priority=1, subset_tags=['b']),
        Policy(priority=2, subset_tags=['a']),
    ) as (multi_connector, _, _):
        # Each policy should see all of the tags in the iterator.
        with pytest.raises(MultiConnectorError, match='constraints'):
            multi_connector.put(b'value', subset_tags=iter(['c']))

        keys = multi_connector.put_batch(
            [b'value1', b'value2'],
            subset_tags=iter(['b']),
        )
        assert [key.connector_name for key in keys] == ['c1', 'c1']


def test_multi_connector_batches_by_connector() -> None:
    with multi_connector_from_policies(
        Policy(max_size_bytes=1),
        Policy(min_size_bytes=2),
    ) as (multi_connector, connector1, connector2):
        objs = [b'a', b'bb', b'c', b'dd', b'e']
        with (
            mock.patch.object(
                connector1,
                'put_batch',
                wraps=connector1.put_batch,
            ) as put1,
            mock.patch.object(
                connector2,
                'put_batch',
                wraps=connector2.put_batch,
            ) as put2,
        ):
            keys = multi_connector.put_batch(objs)
        put1.assert_called_once_with([b'a', b'c', b'e'])
        put2.assert_called_once_with([b'bb', b'dd'])
        assert [key.connector_name for key in keys] == [
            'c1',
            'c2',
            'c1',
            'c2',
            'c1',
        ]

        missing = keys[1]._replace(
            connector_key=connector2.new_key(),
        )
        with (
            mock.patch.object(
                connector1,
                'get_batch',
                wraps=connector1.get_batch,
            ) as get1,
            mock.patch.object(
                connector2,
                'get_batch',
                wraps=connector2.get_batch,
            ) as get2,
        ):
            results = multi_connector.get_batch([*keys, missing])
        assert results == [*objs, None]
        assert get1.call_count == 1
        assert get2.call_count == 1


def test_multi_connector_put_batch_no_valid_puts_nothing() -> None:
    with multi_connector_from_policies(
        Policy(max_size_bytes=1),
        Policy(max_size_bytes=2),
    ) as (multi_connector, connector1, connector2):
        with (
            mock.patch.object(connector1, 'put_batch') as put1,
            mock.patch.object(connector2, 'put_batch') as put2,
            pytest.raises(MultiConnectorError, match='policy'),
        ):
            multi_connector.put_batch([b'a', b'bb', b'ccc'])
        put1.assert_not_called()
        put2.assert_not_called()


def test_multi_connector_get_batch_bad_key() -> None:
    connector = MultiConnector({'connector': (LocalConnector(), Policy())})
    key = connector.put(b'value')
    bad_key = key._replace(connector_name='missing')
    with pytest.raises(MultiConnectorError, match='does not exist'):
        connector.get_batch([key, bad_key])
    connector.close()


def test_multi_connector_policy_no_valid() -> None:
    connectors: dict[str, tuple[Connector[Any], Policy]] = {
        'connector': (LocalConnector(), Policy(max_size_bytes=1)),
    }

    connector = MultiConnector(connectors)
    with pytest.raises(MultiConnectorError, match='policy'):
        connector.put(b'value')
    connector.close()


def test_multi_connector_bad_key() -> None:
    connector = MultiConnector({'connector': (LocalConnector(), Policy())})
    key = connector.put(b'value')
    key = key._replace(connector_name='missing')
    with pytest.raises(MultiConnectorError, match='does not exist'):
        connector.exists(key)


def test_multi_connector_from_config() -> None:
    with multi_connector_from_policies(
        Policy(priority=1, subset_tags=['a', 'b']),
        Policy(priority=2, superset_tags=['x', 'y']),
    ) as (multi_connector, _, _):
        config = multi_connector.config()
        new_connector = MultiConnector.from_config(config)
        assert new_connector.config() == config
        new_connector.close(clear=False)


def test_dormant_connectors() -> None:
    with (
        mock.patch('proxystore.utils.hostname') as mock_hostname,
        multi_connector_from_policies(
            Policy(host_pattern='testhost', subset_tags=['a']),
            Policy(host_pattern='otherhost', subset_tags=['b']),
        ) as (multi_connector, _, _),
    ):
        mock_hostname.return_value = 'otherhost'
        key2 = multi_connector.put(b'data', subset_tags=['b'])
        mock_hostname.return_value = 'testhost'
        key1 = multi_connector.put(b'data', subset_tags=['a'])

        config = multi_connector.config()
        # Reinitalizing the connector from a config will result in
        # the second connector being dormant because it's host pattern
        # did not match the hostname.
        remote_connector = MultiConnector.from_config(config)
        assert remote_connector.exists(key1)

        assert remote_connector.dormant_connectors is not None
        assert len(remote_connector.dormant_connectors) == 1

        # Getting the config should not add active connectors to the
        # dormant connectors.
        remote_connector.config()
        assert len(remote_connector.dormant_connectors) == 1

        with pytest.raises(MultiConnectorError, match='constraints'):
            remote_connector.put(b'data', subset_tags=['b'])
        with pytest.raises(  # pragma: <3.14 cover
            MultiConnectorError,
            match='dormant',
        ):
            remote_connector.get(key2)


@pytest.mark.parametrize('clear', (None, True, False))
def test_multi_connector_close_passes_clear(clear: bool | None) -> None:
    connector1 = mock.MagicMock()
    connector2 = mock.MagicMock()
    connector = MultiConnector(
        {'c1': (connector1, Policy()), 'c2': (connector2, Policy())},
    )
    connector.close(clear=clear)
    for c in (connector1, connector2):
        if clear is None:
            c.close.assert_called_once_with()
        else:
            c.close.assert_called_once_with(clear=clear)


@pytest.mark.parametrize(
    ('results', 'expected'),
    (((False, False), False), ((True, False), True), ((None, True), True)),
)
def test_multi_connector_close_returns_if_any_cleared(
    results: tuple[bool | None, bool | None],
    expected: bool,
) -> None:
    connector1 = mock.MagicMock()
    connector1.close.return_value = results[0]
    connector2 = mock.MagicMock()
    connector2.close.return_value = results[1]
    connector = MultiConnector(
        {'c1': (connector1, Policy()), 'c2': (connector2, Policy())},
    )
    assert connector.close() is expected


def test_multi_connector_close_after_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.ERROR)
    connectors = [mock.MagicMock() for _ in range(3)]
    connectors[0].close.side_effect = RuntimeError('first')
    connectors[1].close.side_effect = RuntimeError('second')
    connector = MultiConnector(
        {f'c{i}': (c, Policy()) for i, c in enumerate(connectors)},
    )

    # The first error is raised once every connector is closed, and later
    # errors are logged.
    with pytest.raises(RuntimeError, match='first'):
        connector.close()
    for c in connectors:
        c.close.assert_called_once_with()
    assert 'Failed to close connector c1' in caplog.text


def _connector_name(key: tuple[Any, ...]) -> str:
    assert isinstance(key, MultiKey)
    return key.connector_name


def test_store_connector_options_route_objects() -> None:
    with multi_connector_from_policies(
        Policy(priority=1, subset_tags=['a']),
        Policy(priority=2, subset_tags=['b']),
    ) as (multi_connector, _, _):
        store = Store(multi_connector, owner=False)

        key = store.put('value', connector_options={'subset_tags': ['a']})
        assert _connector_name(key) == 'c1'

        keys = store.put_batch(
            ['value1', 'value2'],
            connector_options={'subset_tags': ['b']},
        )
        assert all(_connector_name(key) == 'c2' for key in keys)

        proxy = store.proxy('value', connector_options={'subset_tags': ['a']})
        assert _connector_name(get_key(proxy)) == 'c1'

        proxies = store.proxy_batch(
            ['value1', 'value2'],
            connector_options={'subset_tags': ['b']},
        )
        assert all(_connector_name(get_key(p)) == 'c2' for p in proxies)

        with pytest.raises(MultiConnectorError):
            store.put('value', connector_options={'subset_tags': ['c']})

        store.close()
