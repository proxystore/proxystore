"""Utilities for tests in tests/*."""
from __future__ import annotations

import sys

from testing.mocker_modules import pymargo_mocker
from testing.mocker_modules import ucx_mocker

# pymargo and ucx mocker imports

sys.modules['pymargo'] = pymargo_mocker
sys.modules['pymargo.bulk'] = pymargo_mocker
sys.modules['pymargo.core'] = pymargo_mocker
sys.modules['ucp'] = ucx_mocker
