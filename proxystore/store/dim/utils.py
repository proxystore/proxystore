import fcntl
import socket
import struct


def get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        return socket.inet_ntoa(
            fcntl.ioctl(
                s.fileno(),
                0x8915,
                struct.pack("256s", bytes(ifname[:15], "utf-8")),  # SIOCGIFADDR
            )[20:24]
        )
    except OSError as e:
        # Not a solution, but the above doesn't work with Macs
        # need to provide IP rather than the interface name for the time being
        return ifname
