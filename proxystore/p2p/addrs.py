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

from proxystore.endpoint.auth import write_private_file
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.identity import parse_endpoint_id

logger = logging.getLogger(__name__)


def load_peer_addrs(path: str) -> dict[EndpointId, iroh.EndpointAddr]:
    """Load cached peer addresses.

    Args:
        path: Path to the cache file.

    Returns:
        Mapping of peer IDs to addresses. The mapping is empty if the file \
        does not exist or is malformed.
    """
    try:
        with open(path) as f:
            data = json.load(f)
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as e:
        logger.warning('Ignoring malformed peer address cache %s: %s', path, e)
        return {}

    addrs: dict[EndpointId, iroh.EndpointAddr] = {}
    if not isinstance(data, dict):
        logger.warning('Ignoring malformed peer address cache %s', path)
        return addrs
    for key, value in data.items():
        try:
            addrs[parse_endpoint_id(key)] = _decode_addr(key, value)
        except (TypeError, ValueError, iroh.IrohError):
            logger.warning(
                'Ignoring malformed entry for %s in peer address cache %s',
                key,
                path,
            )
    return addrs


def save_peer_addrs(
    path: str,
    addrs: Mapping[EndpointId, iroh.EndpointAddr],
) -> None:
    """Atomically save peer addresses.

    Args:
        path: Path to the cache file.
        addrs: Mapping of peer IDs to addresses.
    """
    data = {
        peer_id: {
            'relay_url': addr.relay_url(),
            'addresses': addr.direct_addresses(),
        }
        for peer_id, addr in sorted(addrs.items())
    }
    write_private_file(path, json.dumps(data, indent=2).encode())


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
