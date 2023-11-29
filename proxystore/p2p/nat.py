"""Tools for getting the NAT type using STUN.

This module is a wrapper around the tools provided by
[pystun3](https://github.com/talkiq/pystun3){target=_blank}.
"""
from __future__ import annotations

import enum
import logging
import socket
from typing import NamedTuple

import stun

logger = logging.getLogger(__name__)

# Selected from the following sources:
#   https://github.com/pradt2/always-online-stun
#   https://gist.github.com/mondain/b0ec1cf5f60ae726202e
_STUN_SERVERS = (
    'stun.l.google.com:19302',
    'stun1.l.google.com:19302',
    'stun2.l.google.com:19302',
    'stun3.l.google.com:19302',
    'stun4.l.google.com:19302',
    'stun.ideasip.com:3478',
    'stun.voiparound.com:3478',
    'stun.voipstunt.com:3478',
)


class NatType(enum.Enum):
    """NAT type.

    Learn more about NAT types at
    https://en.wikipedia.org/wiki/Network_address_translation.
    """

    OpenInternet = 'Open Internet (No NAT)'
    """Host is not behind a NAT."""
    FullCone = 'Full-cone NAT'
    """Host is behind a full-cone NAT."""
    SymmetricUDPFirewall = 'Symmetric UDP Firewall NAT'
    """Host is behind a symmetric UDP firewall."""
    RestrictedCone = 'Restricted-cone NAT'
    """Host is behind a restricted-cone NAT."""
    PortRestrictedCone = 'Port Restricted-cone NAT'
    """Host is behind a port-restricted-cone NAT."""
    Symmetric = 'Symmetric NAT'
    """Host is behind a symmetric NAT."""

    @classmethod
    def _from_str(cls, nat_type: str) -> NatType:
        if nat_type == stun.Blocked:
            raise RuntimeError(
                'No response from a STUN server. '
                'Unable to determine NAT type.',
            )
        elif nat_type == stun.ChangedAddressError:
            raise RuntimeError('Address changed during NAT type check.')

        return {
            stun.OpenInternet: cls.OpenInternet,
            stun.FullCone: cls.FullCone,
            stun.SymmetricUDPFirewall: cls.SymmetricUDPFirewall,
            stun.RestricNAT: cls.RestrictedCone,
            stun.RestricPortNAT: cls.PortRestrictedCone,
            stun.SymmetricNAT: cls.Symmetric,
        }[nat_type]


class Result(NamedTuple):
    """Result of NAT type check.

    Attributes:
        nat_type: Enum type of the found NAT this host is behind.
        external_ip: External IP of this host.
        external_port: External port of this host.
    """

    nat_type: NatType
    external_ip: str
    external_port: int


def check_nat(
    source_ip: str = '0.0.0.0',
    source_port: int = 54320,
) -> Result:
    """Check the NAT type this host is behind.

    This function uses the STUN protocol (RFC 3489) to discover the
    presence and type of the NAT between this host and the open Internet.

    Args:
        source_ip: Address to bind to.
        source_port: Port to listen on.

    Returns:
        Result containing the NAT type and external IP/port of the host.

    Raises:
        RuntimeError: if no STUN servers return a response or if the hosts
            IP or port changed during the STUN process.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.settimeout(2)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((source_ip, source_port))

        for stun_server in _STUN_SERVERS:
            stun_server_address, stun_server_port = stun_server.split(':')

            _nat_type, external = stun.get_nat_type(
                s,
                source_ip,
                source_port,
                stun_host=stun_server_address,
                stun_port=int(stun_server_port),
            )

            try:
                nat_type = NatType._from_str(_nat_type)
            except RuntimeError:
                pass
            else:
                break
        else:
            raise RuntimeError(
                'No STUN servers returned a valid response. '
                'Enable debug level logging for more details.',
            )

    return Result(nat_type, external['ExternalIP'], external['ExternalPort'])


def check_nat_and_log(
    source_ip: str = '0.0.0.0',
    source_port: int = 54320,
) -> None:
    """Check the NAT type this host is behind and log the results.

    Wrapper around [`check_nat()`][proxystore.p2p.nat.check_nat] that logs
    the results rather than return them.

    Args:
        source_ip: Address to bind to.
        source_port: Port to listen on.
    """
    logger.info('Checking NAT type. This may take a moment...')
    try:
        nat_type, external_ip, external_port = check_nat(
            source_ip=source_ip,
            source_port=source_port,
        )
    except Exception as e:
        logger.error(f'Failed to determine NAT type: {e}')
    else:
        logger.info(f'NAT Type:       {nat_type.value}')
        logger.info(f'External IP:    {external_ip}')
        logger.info(f'External Port:  {external_port}')

        if nat_type == NatType.Symmetric:
            logger.warning(
                'NAT traversal (e.g., hole-punching) does not work reliably '
                'across Symmetric NATs or poorly behaved legacy NATs. '
                'Peer-to-peer methods may not work.',
            )
        else:
            logger.info(
                'NAT traversal for peer-to-peer methods (e.g., hole-punching) '
                'is likely to work. (NAT traversal does not work reliably '
                'across symmetric NATs or poorly behaved legacy NATs.)',
            )
