"""Streaming exception types."""
from __future__ import annotations


class TopicClosedError(Exception):
    """Object sent to topic that has already been closed."""

    pass
