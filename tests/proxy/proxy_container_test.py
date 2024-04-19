# This module contains source code from python-lazy-object-proxy v1.10.0
# which is available under the BSD 2-Clause License included below.
#
# The following modifications to the source has been made:
#   * Replaced use of the Proxy type from lazy-object-proxy with the
#     native implementation in ProxyStore.
#   * Consolidated, updated, and/or removed tests.
#   * Formatted code and added type annotations.
#
# Source: https://github.com/ionelmc/python-lazy-object-proxy/tree/v1.10.0
#
# BSD 2-Clause License
#
# Copyright (c) 2014-2023, Ionel Cristian Mărieș. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
from __future__ import annotations

from proxystore.proxy import Proxy


def test_class_bytes() -> None:
    class Class:
        def __bytes__(self):
            return b'BYTES'

    instance = Class()

    proxy = Proxy(lambda: instance)

    assert bytes(instance) == bytes(proxy)


def test_object_hash() -> None:
    def function1(*args, **kwargs):  # pragma: no cover
        return args, kwargs

    function2 = Proxy(lambda: function1)
    assert hash(function2) == hash(function1)


def test_mapping_key() -> None:
    def function1(*args, **kwargs):  # pragma: no cover
        return args, kwargs

    function2 = Proxy(lambda: function1)
    table = {function1: True}
    assert table.get(function2)

    table = {function2: True}
    assert table.get(function1)


def test_index() -> None:
    class TestClass1:
        def __index__(self):
            return 1

    value1 = Proxy(lambda: TestClass1())
    items1 = [0, 1, 2]

    assert items1[value1] == items1[1]

    class TestClass2:
        def __int__(self):
            return 1

    value2 = Proxy(lambda: TestClass2())
    items2 = [0, 1, 2]

    assert items2[value2] == items2[1]


def test_length() -> None:
    value = Proxy(lambda: list(range(3)))
    assert len(value) == 3


def test_contains() -> None:
    value = Proxy(lambda: list(range(3)))
    assert 2 in value
    assert -2 not in value


def test_getitem() -> None:
    value = Proxy(lambda: list(range(3)))
    assert value[1] == 1


def test_setitem() -> None:
    value = Proxy(lambda: list(range(3)))
    value[1] = -1
    assert value[1] == -1


def test_delitem() -> None:
    value = Proxy(lambda: list(range(3)))
    assert len(value) == 3

    del value[1]
    assert len(value) == 2
    assert value[1] == 2


def test_slicing() -> None:
    # Get slice
    value = Proxy(lambda: list(range(5)))
    assert value[1:4] == [1, 2, 3]

    # Set slice
    value[1:4] = reversed(value[1:4])
    assert value[1:4] == [3, 2, 1]

    # Del slice
    del value[1:4]
    assert len(value) == 2
    assert value == [0, 4]


def test_dict_length() -> None:
    value = Proxy(lambda: dict.fromkeys(range(3), False))

    assert len(value) == 3


def test_dict_contains() -> None:
    value = Proxy(lambda: dict.fromkeys(range(3), False))

    assert 2 in value
    assert -2 not in value


def test_dict_getitem() -> None:
    value = Proxy(lambda: dict.fromkeys(range(3), False))

    assert value[1] is False


def test_dict_setitem() -> None:
    value = Proxy(lambda: dict.fromkeys(range(3), False))
    value[1] = True

    assert value[1] is True


def test_dict_delitem() -> None:
    value = Proxy(lambda: dict.fromkeys(range(3), False))

    assert len(value) == 3

    del value[1]

    assert len(value) == 2


def test_list_reversed() -> None:
    instance = [1, 2]
    proxy = Proxy(lambda: instance)
    assert list(reversed(instance)) == list(reversed(proxy))
