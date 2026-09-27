from __future__ import annotations

import asyncio
import contextlib
import pathlib
from collections.abc import AsyncGenerator
from collections.abc import Sequence
from typing import Any

import pytest

from proxystore.connectors.endpoint import EndpointConnector
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import EndpointP2PConfig
from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.p2p.addrs import PeerAddrCache
from proxystore.store.base import Store
from testing.endpoint import write_endpoint


@contextlib.asynccontextmanager
async def _peered_endpoints(
    tmp_path: pathlib.Path,
) -> AsyncGenerator[tuple[list[EndpointConfig], list[str]], None]:
    # Each endpoint has its own ProxyStore home directory to simulate
    # endpoints on different systems.
    homes = [str(tmp_path / 'home1'), str(tmp_path / 'home2')]
    created = [
        write_endpoint(
            home,
            'endpoint',
            host='127.0.0.1',
            p2p=EndpointP2PConfig(enabled=True, relays='none'),
        )
        for home in homes
    ]
    dirs = [endpoint_dir for endpoint_dir, _ in created]
    configs = [config for _, config in created]
    dirs[0].peers.add('peer', configs[1].id)
    dirs[1].peers.add('peer', configs[0].id)

    async with Endpoint(dirs[1]) as endpoint2:
        peer_manager = endpoint2.peer_manager
        # Relays and discovery are disabled so endpoint 1 is given the
        # address of endpoint 2 using the peer address cache. Endpoint 2
        # learns the address of endpoint 1 when endpoint 1 connects to it.
        assert peer_manager is not None
        addr = peer_manager.addr()
        PeerAddrCache(dirs[0].peer_addrs_path).save({configs[1].id: addr})
        async with Endpoint(dirs[0]):
            yield configs, homes


def _store(name: str, endpoints: Sequence[str], home: str) -> Store[Any]:
    connector = EndpointConnector(endpoints, proxystore_dir=home)
    return Store(name, connector, register=False)


@pytest.mark.integration
async def test_endpoint_transfer(tmp_path: pathlib.Path) -> None:
    async with _peered_endpoints(tmp_path) as (configs, homes):
        endpoints = [config.id for config in configs]

        def _transfer() -> None:
            with (
                _store('store1', endpoints, homes[0]) as store1,
                _store('store2', endpoints, homes[1]) as store2,
            ):
                # Endpoint 1 requests from endpoint 2
                key = store2.put([1, 2, 3])
                assert store1.get(key) == [1, 2, 3]
                store1.evict(key)
                assert not store2.exists(key)

                # Endpoint 2 requests from endpoint 1 using the address it
                # learned from the previous connection.
                key = store1.put('value')
                assert store2.get(key) == 'value'
                assert store2.exists(key)

        await asyncio.to_thread(_transfer)
