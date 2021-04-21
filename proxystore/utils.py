"""Utility Functions for Proxies"""
from typing import Any, Optional

from proxystore.proxy import Proxy


def get_key(proxy: Proxy) -> Optional[str]:
    """Returns key associated object wrapped by proxy

    Args:
        proxy (Proxy)

    Returns:
        key (str) if it exists otherwise `None`
    """
    if hasattr(proxy.__factory__, 'key'):
        return proxy.__factory__.key
    return None


def extract(proxy: Proxy) -> Any:
    """Returns object wrapped by proxy"""
    return proxy.__wrapped__


def is_resolved(proxy: Proxy) -> bool:
    """Check if a proxy is resolved"""
    return proxy.__resolved__


def resolve_async(proxy: Proxy) -> None:
    """Begin resolving proxy asynchronously"""
    if not is_resolved(proxy):
        proxy.__factory__.resolve_async()
