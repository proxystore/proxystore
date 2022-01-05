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


def fullname(obj):
    """Returns full name of object"""
    if hasattr(obj, '__module__'):
        module = obj.__module__
    else:
        module = obj.__class__.__module__
    if hasattr(obj, '__name__'):
        name = obj.__name__
    else:
        name = obj.__class__.__name__
    if module is None or module == str.__module__:
        return name
    return f'{module}.{name}'
