"""MultiStore Implementation."""
from __future__ import annotations

import dataclasses
import sys
from typing import Iterable

if sys.version_info >= (3, 8):  # pragma: >=3.8 cover
    from typing import TypedDict
else:  # pragma: <3.8 cover
    from typing_extensions import TypedDict


class PolicyDict(TypedDict):
    """JSON compatible representation of a `~Policy`."""

    priority: int
    min_size: int
    max_size: int
    subset_tags: list[str]
    superset_tags: list[str]


@dataclasses.dataclass
class Policy:
    """Policy that allows validating a set of constraints."""

    priority: int = 0
    min_size: int = 0
    max_size: int = sys.maxsize
    subset_tags: list[str] = dataclasses.field(default_factory=list)
    superset_tags: list[str] = dataclasses.field(default_factory=list)

    def is_valid(
        self,
        *,
        size: int | None = None,
        subset_tags: Iterable[str] | None = None,
        superset_tags: Iterable[str] | None = None,
    ) -> bool:
        """Check if set of contstraints is valid for this policy.

        Note:
            All arguments are optional keyword arguments that default to
            ``None``. If left as the default, that constraint will not be
            checked against the policy.

        Args:
            size (int): object size.
            subset_tags (Iterable[str]): set of tags that must be a subset
                of the Policy's ``subset_tags`` to be valid.
            superset_tags (Iterable[str]): set of tags that must be a superset
                of the Policy's ``superset_tags`` to be valid.

        Returns:
            if the provided constraints are valid for the policy.
        """
        if size is not None and (size < self.min_size or size > self.max_size):
            return False
        if subset_tags is not None and not set(subset_tags).issubset(
            self.subset_tags,
        ):
            return False
        if superset_tags is not None and not set(superset_tags).issuperset(
            self.superset_tags,
        ):
            return False
        return True

    def as_dict(self) -> PolicyDict:
        """Convert the Policy to a JSON compatible dict.

        Usage:
            >>> policy = Policy(...)
            >>> policy_dict = policy.as_dict()
            >>> Policy(**policy_dict) == policy
            True
        """
        # We could use dataclasses.asdict(self) but this gives us the benefit
        # of typing on the return dict.
        return PolicyDict(
            priority=self.priority,
            min_size=self.min_size,
            max_size=self.max_size,
            subset_tags=self.subset_tags,
            superset_tags=self.superset_tags,
        )
