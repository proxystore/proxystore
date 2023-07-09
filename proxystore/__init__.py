"""ProxyStore is a library for decoupling object communication from code.

**Legacy Documentation:** Documentation for ProxyStore versions older than
v0.4.1 are hosted at [proxystore.readthedocs.io/](https://proxystore.readthedocs.io/){target=_blank}.
"""
from __future__ import annotations

import importlib.metadata as importlib_metadata
import sys

__version__ = importlib_metadata.version('proxystore')
