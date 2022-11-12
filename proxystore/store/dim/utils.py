"""Shared functions used by DIM stores."""
from __future__ import annotations

import fcntl
import socket
import struct
from typing import NamedTuple


def get_ip_address(ifname: str) -> str:
    """Get ip address provided an interface name.

    Args:
        ifname (str): interface name

    Returns:
        the IP address (str)

    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
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
    except OSError:
        # Not a solution, but the above doesn't work with Macs
        # need to provide IP rather than the interface name for the time being
        return ifname


class Status(NamedTuple):
    """Task status response."""

    success: bool
    error: Exception | None = None
