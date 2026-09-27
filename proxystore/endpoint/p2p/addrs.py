"""Cache of peer addresses.

Warning:
    This module is an internal implementation detail. Its interface may
    change between releases without notice (see
    [`proxystore.endpoint`][proxystore.endpoint]).

The address of a peer (its home relay URL and direct addresses) is saved
after each successful connection so the peer can be reached later even if
discovery is unavailable, as long as the addresses of the peer have not
changed.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping

import iroh
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator

from proxystore.endpoint.exceptions import EndpointConfigError
from proxystore.endpoint.files import check_format_version
from proxystore.endpoint.files import read_json_model
from proxystore.endpoint.files import write_json_model
from proxystore.endpoint.identity import EndpointId

logger = logging.getLogger(__name__)

ADDR_CACHE_VERSION = 1
"""Format version of the peer address cache file."""


class PeerAddr(BaseModel):
    """Cached address of a peer.

    Attributes:
        relay_url: URL of the home relay of the peer.
        addresses: Direct addresses of the peer.
    """

    model_config = ConfigDict(extra='forbid')

    relay_url: str | None = None
    addresses: list[str] = Field(default_factory=list)

    @classmethod
    def from_iroh(cls, addr: iroh.EndpointAddr) -> PeerAddr:
        """Create from an iroh address."""
        return cls(
            relay_url=addr.relay_url(),
            addresses=addr.direct_addresses(),
        )

    def to_iroh(self, peer_id: EndpointId) -> iroh.EndpointAddr:
        """Convert to an iroh address of the peer."""
        return iroh.EndpointAddr(
            iroh.EndpointId.from_string(peer_id),
            self.relay_url,
            self.addresses,
        )


class PeerAddrCacheFile(BaseModel):
    """Contents of the peer address cache file.

    Attributes:
        version: Format version of the file.
        peers: Mapping of peer IDs to addresses.
    """

    model_config = ConfigDict(extra='forbid')

    version: int = ADDR_CACHE_VERSION
    peers: dict[EndpointId, PeerAddr] = Field(default_factory=dict)

    @field_validator('version')
    @classmethod
    def _version_validator(cls, v: int) -> int:
        return check_format_version(
            v,
            ADDR_CACHE_VERSION,
            'peer address cache',
        )


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

        The cache is only an optimization so errors are logged rather than
        raised.

        Returns:
            Mapping of peer IDs to addresses. The mapping is empty if the \
            file does not exist, is malformed, or has an unsupported format \
            version.
        """
        try:
            cache = read_json_model(
                PeerAddrCacheFile,
                self.path,
                'peer address cache',
            )
        except FileNotFoundError:
            return {}
        except (OSError, EndpointConfigError) as e:
            logger.warning('Ignoring peer address cache: %s', e)
            return {}

        return {
            peer_id: addr.to_iroh(peer_id)
            for peer_id, addr in cache.peers.items()
        }

    def save(self, addrs: Mapping[EndpointId, iroh.EndpointAddr]) -> None:
        """Atomically save peer addresses, replacing the cached addresses.

        Args:
            addrs: Mapping of peer IDs to addresses.
        """
        peers = {
            peer_id: PeerAddr.from_iroh(addr)
            for peer_id, addr in sorted(addrs.items())
        }
        write_json_model(self.path, PeerAddrCacheFile(peers=peers))
