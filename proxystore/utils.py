"""Utility functions"""
import random

from typing import Any


def create_key(obj: Any) -> str:
    """Generates key for the object

    .. todo::

       * generate key based on object hash (Re: Issue #4)
       * consider how to deal with key collisions

    Args:
        obj: object to create key for

    Returns:
        random 128 bit string.
    """
    return str(random.getrandbits(128))
