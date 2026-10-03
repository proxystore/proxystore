from __future__ import annotations

import re
from unittest import mock

import pytest

from proxystore.stream.filters import NullFilter
from proxystore.stream.filters import SamplingFilter


def test_null_filter():
    keep = NullFilter()
    assert keep({})
    assert keep({'field': True})


def test_sampling_filter():
    keep = SamplingFilter(0.2)

    with mock.patch('random.random', return_value=0.1):
        assert keep({})
        assert keep({'field': True})

    with mock.patch('random.random', return_value=0.3):
        assert not keep({})
        assert not keep({'field': True})


def test_sampling_filter_bounds():
    with mock.patch('random.random', return_value=0.0):
        assert not SamplingFilter(0)({})
    with mock.patch('random.random', return_value=0.999):
        assert SamplingFilter(1)({})


def test_sampling_filter_value_error():
    with pytest.raises(ValueError, match=re.escape('[0, 1]')):
        SamplingFilter(-1)

    with pytest.raises(ValueError, match=re.escape('[0, 1]')):
        SamplingFilter(1.1)
