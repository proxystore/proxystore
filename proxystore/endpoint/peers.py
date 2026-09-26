"""Peer endpoint allowlist.

An endpoint only communicates with the peer endpoints in its allowlist,
the `peers.toml` file in the endpoint directory. Allowlisting is
symmetric: two endpoints can only communicate if each endpoint lists the
other.

```toml title="peers.toml"
[peers]
my-laptop = "5d3e...a1f2"
cluster = "bb04...9c3e"
```

The allowlist can be changed while the endpoint is running. The endpoint
reloads the file when it changes so a removed peer is denied access
immediately.
"""

from __future__ import annotations

import dataclasses
import logging
import os
from typing import Any

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator

from proxystore.endpoint.config import validate_name
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.identity import parse_endpoint_id
from proxystore.utils.config import load

logger = logging.getLogger(__name__)


class PeersConfig(BaseModel):
    """Allowlist of peer endpoints.

    Attributes:
        peers: Mapping of peer names to endpoint IDs. Names are only used to
            help users manage the allowlist.

    Raises:
        ValueError: If a name does not contain only alphanumeric, dash, or
            underscore characters, if an endpoint ID is invalid, or if an
            endpoint ID is listed more than once.
    """

    model_config = ConfigDict(extra='forbid')

    peers: dict[str, EndpointId] = Field(default_factory=dict)

    @field_validator('peers', mode='before')
    @classmethod
    def _peers_validator(cls, v: Any) -> dict[str, EndpointId]:
        if not isinstance(v, dict):
            raise ValueError('Peers must be a table of names to endpoint IDs.')
        peers: dict[str, EndpointId] = {}
        seen: dict[EndpointId, str] = {}
        for name, value in v.items():
            if not validate_name(name):
                raise ValueError(
                    'Peer names must only contain alphanumeric characters, '
                    f'dashes, and underscores. Got {name}.',
                )
            endpoint_id = parse_endpoint_id(value)
            if endpoint_id in seen:
                raise ValueError(
                    f'Peers {seen[endpoint_id]} and {name} have the same '
                    f'endpoint ID ({endpoint_id}).',
                )
            seen[endpoint_id] = name
            peers[name] = endpoint_id
        return peers

    def name_of(self, endpoint_id: EndpointId) -> str | None:
        """Get the name of the peer with the endpoint ID."""
        for name, peer_id in self.peers.items():
            if peer_id == endpoint_id:
                return name
        return None


@dataclasses.dataclass(frozen=True)
class _FileState:
    mtime_ns: int
    size: int
    inode: int


class Allowlist:
    """Allowlist of peer endpoints backed by a `peers.toml` file.

    The file is reloaded when it changes. A missing file is an empty
    allowlist. If the file is malformed, all peers are denied until it is
    fixed.

    Args:
        path: Path to the `peers.toml` file.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        self._state: _FileState | None = None
        self._peers = PeersConfig()

    def reload(self) -> set[EndpointId]:
        """Reload the allowlist if the file changed.

        Returns:
            Endpoint IDs that were removed from the allowlist.
        """
        try:
            stat = os.stat(self.path)
        except FileNotFoundError:
            state = None
        else:
            state = _FileState(stat.st_mtime_ns, stat.st_size, stat.st_ino)

        if state == self._state:
            return set()
        self._state = state

        old = set(self._peers.peers.values())
        if state is None:
            self._peers = PeersConfig()
        else:
            try:
                self._peers = read_peers(self.path)
            except ValueError:
                logger.exception(
                    'Failed to load peer allowlist from %s. All peers will '
                    'be denied until the file is fixed',
                    self.path,
                )
                self._peers = PeersConfig()
        return old - set(self._peers.peers.values())

    @property
    def peers(self) -> PeersConfig:
        """Current allowlist, reloaded if the file changed."""
        self.reload()
        return self._peers

    def allowed(self, endpoint_id: EndpointId) -> bool:
        """Check if the endpoint is in the allowlist."""
        return endpoint_id in self.peers.peers.values()

    def name_of(self, endpoint_id: EndpointId) -> str | None:
        """Get the name of the peer or `None` if it is not allowed."""
        return self.peers.name_of(endpoint_id)


def read_peers(path: str) -> PeersConfig:
    """Read a peer allowlist file.

    Args:
        path: Path to the `peers.toml` file.

    Returns:
        The allowlist or an empty allowlist if the file does not exist.

    Raises:
        ValueError: If the file cannot be parsed or is invalid.
    """
    try:
        with open(path, 'rb') as f:
            return load(PeersConfig, f)
    except FileNotFoundError:
        return PeersConfig()
    except ValueError as e:
        raise ValueError(f'Unable to parse ({path}): {e!s}.') from None
