"""Endpoint warning types."""

from __future__ import annotations


class EndpointVersionWarning(Warning):
    """Client and endpoint use different ProxyStore or Python versions."""

    pass
