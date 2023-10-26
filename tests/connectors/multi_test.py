from __future__ import annotations

import contextlib
import json
from typing import Any
from typing import Generator
from unittest import mock

import pytest

from proxystore.connectors.local import LocalConnector
from proxystore.connectors.multi import MultiConnector
from proxystore.connectors.multi import MultiConnectorError
from proxystore.connectors.multi import Policy
from proxystore.connectors.protocols import Connector


@contextlib.contextmanager
def multi_connector_from_policies(
    p1: Policy,
    p2: Policy,
) -> Generator[
    tuple[MultiConnector, LocalConnector, LocalConnector],
    None,
    None,
]:
    connector1 = LocalConnector()
    connector2 = LocalConnector()

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
    ) as (multi_connector, connector1, connector2):
        config = multi_connector.config()
        MultiConnector.from_config(config)


def test_dormant_connectors() -> None:
    with mock.patch('proxystore.utils.hostname') as mock_hostname:
        with multi_connector_from_policies(
            Policy(host_pattern='testhost', subset_tags=['a']),
            Policy(host_pattern='otherhost', subset_tags=['b']),
        ) as (multi_connector, connector1, connector2):
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

            with pytest.raises(MultiConnectorError, match='constraints'):
                remote_connector.put(b'data', subset_tags=['b'])
            with pytest.raises(MultiConnectorError, match='dormant'):
                remote_connector.get(key2)
