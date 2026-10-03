"""Warning types."""

from __future__ import annotations


class ExperimentalWarning(Warning):
    """ProxyStore experimental feature warning."""


class VersionMismatchWarning(Warning):
    """Processes which exchange objects use different versions.

    Objects serialized by one version of ProxyStore or Python may fail to
    deserialize with another. For example, a client and the endpoint it
    connects to use different ProxyStore versions or Python minor versions.

    Proxies, store and connector configurations, and stream events created
    by one 2.x version of ProxyStore can be used with any other 2.x version.
    Fields added by a newer version are ignored by an older version with
    this warning.
    """
