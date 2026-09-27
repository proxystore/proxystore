"""Cache of peer addresses.

The address of a peer (its home relay URL and direct addresses) is saved
after each successful connection so the peer can be reached later even if
discovery is unavailable, as long as the addresses of the peer have not
changed.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any

import iroh

from proxystore.endpoint.files import write_private_file
from proxystore.endpoint.identity import EndpointId

logger = logging.getLogger(__name__)

ADDR_CACHE_VERSION = 1
"""Format version of the peer address cache file."""


class PeerAddrCache:
    """Cache of peer addresses stored in a JSON file.

    Example:
        ```python
        cache = PeerAddrCache(endpoint_dir.peer_addrs_path)
        cache.save({peer_id: addr})
        assert peer_id in cache.load()
        ```

    Args:
        path: Path to the cache file.
    """

    def __init__(self, path: str) -> None:
        self.path = path

    def load(self) -> dict[EndpointId, iroh.EndpointAddr]:
        """Load cached peer addresses.

        Returns:
            Mapping of peer IDs to addresses. The mapping is empty if the \
            file does not exist or is malformed.
        """
        path = self.path
        try:
            with open(path) as f:
                data = json.load(f)
        except FileNotFoundError:
            return {}
        except (OSError, ValueError) as e:
            logger.warning(
                'Ignoring malformed peer address cache %s: %s',
                path,
                e,
            )
            return {}

        addrs: dict[EndpointId, iroh.EndpointAddr] = {}
        if not isinstance(data, dict) or not isinstance(
            data.get('peers'),
            dict,
        ):
            logger.warning('Ignoring malformed peer address cache %s', path)
            return addrs
        if data.get('version') != ADDR_CACHE_VERSION:
            # The cache is only an optimization so it is safe to ignore.
            logger.warning(
                'Ignoring peer address cache %s with unsupported format '
                'version %r',
                path,
                data.get('version'),
            )
            return addrs
        for key, value in data['peers'].items():
            try:
                addrs[EndpointId.from_str(key)] = _decode_addr(key, value)
            except (TypeError, ValueError, iroh.IrohError):
                logger.warning(
                    'Ignoring malformed entry for %s in peer address cache %s',
                    key,
                    path,
                )
        return addrs

    def save(self, addrs: Mapping[EndpointId, iroh.EndpointAddr]) -> None:
        """Atomically save peer addresses, replacing the cached addresses.

        Args:
            addrs: Mapping of peer IDs to addresses.
        """
        peers = {
            peer_id: {
                'relay_url': addr.relay_url(),
                'addresses': addr.direct_addresses(),
            }
            for peer_id, addr in sorted(addrs.items())
        }
        data = {'version': ADDR_CACHE_VERSION, 'peers': peers}
        write_private_file(self.path, json.dumps(data, indent=2).encode())


def _decode_addr(key: str, value: Any) -> iroh.EndpointAddr:
    if not isinstance(value, dict):
        raise TypeError('Expected an object.')
    relay_url = value.get('relay_url')
    addresses = value.get('addresses', [])
    if relay_url is not None and not isinstance(relay_url, str):
        raise TypeError('Expected relay_url to be a string or null.')
    if not isinstance(addresses, list) or not all(
        isinstance(a, str) for a in addresses
    ):
        raise TypeError('Expected addresses to be a list of strings.')
    return iroh.EndpointAddr(
        iroh.EndpointId.from_string(key),
        relay_url,
        addresses,
    )
