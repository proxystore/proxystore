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
from proxystore.endpoint.files import check_format_version
from proxystore.endpoint.files import write_private_file
from proxystore.endpoint.identity import EndpointId
from proxystore.utils.config import dumps
from proxystore.utils.config import load

logger = logging.getLogger(__name__)


PEERS_VERSION = 1
"""Format version of the peers file."""


class PeersConfig(BaseModel):
    """Allowlist of peer endpoints.

    Attributes:
        version: Format version of the peers file.
        peers: Mapping of peer names to endpoint IDs. Names are only used to
            help users manage the allowlist.

    Raises:
        ValueError: If a name does not contain only alphanumeric, dash, or
            underscore characters, if an endpoint ID is invalid, if an
            endpoint ID is listed more than once, or if the version is not
            supported.
    """

    model_config = ConfigDict(extra='forbid')

    version: int = PEERS_VERSION
    peers: dict[str, EndpointId] = Field(default_factory=dict)

    @field_validator('version')
    @classmethod
    def _version_validator(cls, v: int) -> int:
        return check_format_version(v, PEERS_VERSION, 'peers file')

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
            endpoint_id = EndpointId.from_str(value)
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


class PeerExistsError(ValueError):
    """A peer with the name already exists."""


class Peers:
    """Peers of an endpoint stored in its `peers.toml` file.

    Example:
        ```python
        peers = EndpointDir.from_name('my-ep').peers
        peers.add('laptop', 'ed92...')
        assert peers.read().peers == {'laptop': 'ed92...'}
        peers.remove('laptop')
        ```

    Args:
        path: Path to the `peers.toml` file.
        owner_id: ID of the endpoint which owns the peers. Used to prevent
            an endpoint from adding itself as a peer.
    """

    def __init__(self, path: str, *, owner_id: EndpointId | None = None):
        self.path = path
        self.owner_id = owner_id

    def read(self) -> PeersConfig:
        """Read the peers.

        Returns:
            The peers or no peers if the file does not exist.

        Raises:
            ValueError: If the file cannot be parsed or is invalid.
        """
        try:
            with open(self.path, 'rb') as f:
                return load(PeersConfig, f)
        except FileNotFoundError:
            return PeersConfig()
        except ValueError as e:
            raise ValueError(
                f'Unable to parse ({self.path}): {e!s}.'
            ) from None

    def write(self, peers: PeersConfig) -> None:
        """Atomically write the peers."""
        write_private_file(self.path, dumps(peers).encode())

    def add(self, name: str, endpoint_id: str) -> EndpointId:
        """Add a peer.

        Args:
            name: Name of the peer.
            endpoint_id: ID of the peer endpoint.

        Returns:
            The ID of the peer.

        Raises:
            PeerExistsError: If a peer with the name already exists.
            ValueError: If the name or ID is invalid, the ID is the ID of the
                owner, the endpoint is already a peer with a different name,
                or the file cannot be parsed.
        """
        if not validate_name(name):
            raise ValueError(
                'Peer names must only contain alphanumeric characters, '
                f'dashes, and underscores. Got {name}.',
            )
        peer_id = EndpointId.from_str(endpoint_id)
        if peer_id == self.owner_id:
            raise ValueError('An endpoint cannot be a peer of itself.')
        peers = self.read()
        if name in peers.peers:
            raise PeerExistsError(f'A peer named {name} already exists.')
        existing = peers.name_of(peer_id)
        if existing is not None:
            raise ValueError(
                f'Endpoint {peer_id} is already a peer named {existing}.',
            )
        peers.peers[name] = peer_id
        self.write(peers)
        return peer_id

    def remove(self, name: str) -> EndpointId:
        """Remove a peer.

        If the endpoint is running, the peer is denied access immediately.

        Args:
            name: Name of the peer.

        Returns:
            The ID of the removed peer.

        Raises:
            ValueError: If there is no peer with the name or the file cannot
                be parsed.
        """
        peers = self.read()
        peer_id = peers.peers.pop(name, None)
        if peer_id is None:
            raise ValueError(f'No peer named {name}.')
        self.write(peers)
        return peer_id

    def allowlist(self) -> Allowlist:
        """Get an allowlist which reloads the peers when the file changes."""
        return Allowlist(self.path)


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
                self._peers = Peers(self.path).read()
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
