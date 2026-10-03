"""Warning types."""

from __future__ import annotations


class ExperimentalWarning(Warning):
    """ProxyStore experimental feature warning."""


class VersionMismatchWarning(Warning):
    """Processes which exchange objects use different versions.

    Objects serialized by one version of ProxyStore or Python may fail to
    deserialize with another. For example, a client and the endpoint it
    connects to use different ProxyStore versions or Python minor versions.
    """
