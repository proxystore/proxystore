"""Warning types."""

from __future__ import annotations


class ExperimentalWarning(Warning):
    """ProxyStore experimental feature warning."""

    pass


class EndpointVersionWarning(Warning):
    """Client and endpoint use different ProxyStore or Python versions."""

    pass
