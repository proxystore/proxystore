"""Shared functions used by DIM stores."""
from __future__ import annotations

import fcntl
import socket
import struct


def get_ip_address(ifname: str) -> str:  # pragma: darwin no cover
    """Get ip address provided an interface name.

    Warning:
        This function does not work on MacOS/Darwin.

    Args:
        ifname: The interface name.

    Returns:
        The IP address.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(
        fcntl.ioctl(
            s.fileno(),
            0x8915,
            struct.pack(
                '256s',
                bytes(ifname[:15], 'utf-8'),
            ),  # SIOCGIFADDR
        )[20:24],
    )
