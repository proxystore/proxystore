from __future__ import annotations

import uuid
from collections.abc import Generator

import pytest

from proxystore.connectors.daos import DAOSConnector
from proxystore.connectors.daos import DAOSKey


@pytest.fixture
def connector() -> Generator[DAOSConnector, None, None]:
    with DAOSConnector(
        pool=str(uuid.uuid4()),
        container=str(uuid.uuid4()),
        namespace=str(uuid.uuid4()),
    ) as connector:
        yield connector


def test_validate_key(connector: DAOSConnector) -> None:
    fake_key = DAOSKey(
        pool=str(uuid.uuid4()),
        container=str(uuid.uuid4()),
        namespace=str(uuid.uuid4()),
        dict_key=str(uuid.uuid4()),
    )

    with pytest.raises(ValueError, match='key do not match the connector'):
        connector.evict(fake_key)

    with pytest.raises(ValueError, match='key do not match the connector'):
        connector.exists(fake_key)

    with pytest.raises(ValueError, match='key do not match the connector'):
        connector.get(fake_key)

    with pytest.raises(ValueError, match='key do not match the connector'):
        connector.get_batch([fake_key])

    with pytest.raises(ValueError, match='key do not match the connector'):
        connector.set(fake_key, b'value')


def test_close_persists_keys_by_default(connector: DAOSConnector) -> None:
    key = connector.put(b'value')
    connector.close()
    assert connector.get(key) == b'value'


def test_close_clear(connector: DAOSConnector) -> None:
    key = connector.put(b'value')
    connector.close(clear=True)
    assert connector.get(key) is None
