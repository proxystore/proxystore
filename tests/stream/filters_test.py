from __future__ import annotations

from unittest import mock

import pytest

from proxystore.stream.filters import NullFilter
from proxystore.stream.filters import SamplingFilter


def test_null_filter():
    filter_ = NullFilter()
    assert not filter_({})
    assert not filter_({'field': True})


def test_sampling_filter():
    filter_ = SamplingFilter(0.2)

    with mock.patch('random.random', return_value=0.1):
        assert filter_({})
        assert filter_({'field': True})

    with mock.patch('random.random', return_value=0.3):
        assert not filter_({})
        assert not filter_({'field': True})


def test_sampling_filter_value_error():
    with pytest.raises(ValueError, match='[0, 1]'):
        SamplingFilter(-1)

    with pytest.raises(ValueError, match='[0, 1]'):
        SamplingFilter(1.1)
