"""Set of common stream filters."""
from __future__ import annotations

import random
from typing import Any


class NullFilter:
    """Filter which never filters out objects."""

    def __call__(self, metadata: dict[str, Any]) -> bool:
        """Apply the filter to event metadata."""
        return False


class SamplingFilter:
    """Filter that randomly filters out objects.

    Args:
        p: Probability of the filter return `True`. I.e., the object gets
            filtered out.

    Raises:
        ValueError: if `p` is not in the range `[0, 1]`.
    """

    def __init__(self, p: float) -> None:
        if p < 0 or p > 1:
            raise ValueError(
                f'Filter probability p must be in [0, 1]. Got p={p}.',
            )
        self._p = p

    def __call__(self, metadata: dict[str, Any]) -> bool:
        """Apply the filter to event metadata."""
        return random.random() <= self._p
