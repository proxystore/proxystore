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
checks if the file changed at most once per second (see
[`Allowlist`][proxystore.endpoint.peers.Allowlist]) so a removed peer is
denied access within about a second.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import time
from typing import Any
from typing import ClassVar

from pydantic import Field
from pydantic import field_validator

from proxystore.endpoint.config import check_name
from proxystore.endpoint.exceptions import EndpointConfigError
from proxystore.endpoint.exceptions import PeerExistsError
from proxystore.endpoint.exceptions import PeerNotFoundError
from proxystore.endpoint.files import read_model
from proxystore.endpoint.files import VersionedFile
from proxystore.endpoint.files import write_model
from proxystore.endpoint.identity import EndpointId

logger = logging.getLogger(__name__)


PEERS_VERSION = 1
"""Format version of the peers file."""
RELOAD_INTERVAL = 1.0
"""Default minimum seconds between checks for changes to the peers file."""


class PeersConfig(VersionedFile):
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

    DESCRIPTION: ClassVar[str] = 'peers file'

    version: int = PEERS_VERSION
    peers: dict[str, EndpointId] = Field(default_factory=dict)

    @field_validator('peers', mode='before')
    @classmethod
    def _peers_validator(cls, v: Any) -> dict[str, EndpointId]:
        if not isinstance(v, dict):
            raise ValueError('Peers must be a table of names to endpoint IDs.')
        peers: dict[str, EndpointId] = {}
        seen: dict[EndpointId, str] = {}
        for name, value in v.items():
            check_name(name, 'Peer')
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


class Peers:
    """Peers of an endpoint stored in its `peers.toml` file.

    Example:
        ```python
        peers = EndpointDir.from_name('my-ep').peers()
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
            EndpointConfigError: If the file cannot be parsed or is invalid.
        """
        try:
            return read_model(PeersConfig, self.path)
        except FileNotFoundError:
            return PeersConfig()

    def write(self, peers: PeersConfig) -> None:
        """Atomically write the peers."""
        write_model(self.path, peers)

    def add(self, name: str, endpoint_id: str) -> EndpointId:
        """Add a peer.

        Args:
            name: Name of the peer.
            endpoint_id: ID of the peer endpoint.

        Returns:
            The ID of the peer.

        Raises:
            PeerExistsError: If a peer with the name already exists.
            EndpointConfigError: If the name or ID is invalid, the ID is the
                ID of the owner, the endpoint is already a peer with a
                different name, or the file cannot be parsed.
        """
        try:
            check_name(name, 'Peer')
            peer_id = EndpointId.from_str(endpoint_id)
        except ValueError as e:
            raise EndpointConfigError(str(e)) from None
        if peer_id == self.owner_id:
            raise EndpointConfigError(
                'An endpoint cannot be a peer of itself.',
            )
        peers = self.read()
        if name in peers.peers:
            raise PeerExistsError(f'A peer named {name} already exists.')
        existing = peers.name_of(peer_id)
        if existing is not None:
            raise EndpointConfigError(
                f'Endpoint {peer_id} is already a peer named {existing}.',
            )
        peers.peers[name] = peer_id
        self.write(peers)
        return peer_id

    def remove(self, name: str) -> EndpointId:
        """Remove a peer.

        If the endpoint is running, the peer is denied access within about
        a second (see [`Allowlist`][proxystore.endpoint.peers.Allowlist]).

        Args:
            name: Name of the peer.

        Returns:
            The ID of the removed peer.

        Raises:
            PeerNotFoundError: If there is no peer with the name.
            EndpointConfigError: If the file cannot be parsed.
        """
        peers = self.read()
        peer_id = peers.peers.pop(name, None)
        if peer_id is None:
            raise PeerNotFoundError(f'No peer named {name}.')
        self.write(peers)
        return peer_id


@dataclasses.dataclass(frozen=True)
class _FileState:
    mtime_ns: int
    size: int
    inode: int


class Allowlist:
    """Allowlist of peer endpoints backed by a `peers.toml` file.

    This is the
    [`PeerPolicy`][proxystore.endpoint.p2p.manager.PeerPolicy] of endpoints.
    The file is reloaded when it changes. A missing file is an empty
    allowlist. If the file is malformed, all peers are denied until it is
    fixed.

    Checking if the file changed requires a `stat()` call which can be slow
    on network file systems, so the file is checked at most once every
    `reload_interval` seconds rather than on every request.

    Args:
        path: Path to the `peers.toml` file.
        reload_interval: Minimum seconds between checks for changes to the
            file. If `0`, the file is checked each time the allowlist is
            used.
    """

    def __init__(
        self,
        path: str,
        *,
        reload_interval: float = RELOAD_INTERVAL,
    ) -> None:
        self.path = path
        self.reload_interval = reload_interval
        self._checked: float | None = None
        self._state: _FileState | None = None
        self._peers = PeersConfig()

    def reload(self, *, force: bool = False) -> None:
        """Reload the allowlist if the file changed.

        Args:
            force: Check if the file changed even if the reload interval has
                not passed since the last check.
        """
        now = time.monotonic()
        if (
            not force
            and self._checked is not None
            and now - self._checked < self.reload_interval
        ):
            return
        self._checked = now

        try:
            stat = os.stat(self.path)
        except FileNotFoundError:
            state = None
        else:
            state = _FileState(stat.st_mtime_ns, stat.st_size, stat.st_ino)

        if state == self._state:
            return
        self._state = state

        if state is None:
            self._peers = PeersConfig()
        else:
            try:
                self._peers = Peers(self.path).read()
            except ValueError as e:
                logger.error(
                    'Failed to load peer allowlist from %s. All peers will '
                    'be denied until the file is fixed: %s',
                    self.path,
                    e,
                )
                self._peers = PeersConfig()

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
