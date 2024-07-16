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

import decimal
import fractions

from proxystore.proxy import Proxy


def test_comparison() -> None:
    one = Proxy(lambda: 1)
    two = Proxy(lambda: 2)
    three = Proxy(lambda: 3)

    assert two > 1
    assert two >= 1
    assert two < 3
    assert two <= 3
    assert two != 1
    assert two == 2
    assert two != 3

    assert one < 2
    assert one <= 2
    assert three > 2
    assert three >= 2
    assert one != 2
    assert two == 2
    assert three != 2

    assert two > one
    assert two >= one
    assert two < three
    assert two <= three
    assert two != one
    assert two == two
    assert two != three


def test_numeric_conversion() -> None:
    one = Proxy(lambda: 1)
    assert int(one) == 1
    assert float(one) == 1.0

    value = Proxy(lambda: 0)
    assert not bool(value)

    proxy = Proxy(lambda: 1.2)
    assert round(proxy) == 1


def test_add() -> None:
    one = Proxy(lambda: 1)
    two = Proxy(lambda: 2)

    assert one + two == 1 + 2
    assert 1 + two == 1 + 2
    assert one + 2 == 1 + 2


def test_sub() -> None:
    one = Proxy(lambda: 1)
    two = Proxy(lambda: 2)

    assert one - two == 1 - 2
    assert 1 - two == 1 - 2
    assert one - 2 == 1 - 2


def test_mul() -> None:
    two = Proxy(lambda: 2)
    three = Proxy(lambda: 3)

    assert two * three == 2 * 3
    assert 2 * three == 2 * 3
    assert two * 3 == 2 * 3


def test_matmul() -> None:
    class MatmulClass:
        def __init__(self, value):
            self.value = value

        def __matmul__(self, other):
            return self.value * other.value

        def __rmatmul__(self, other):
            return other + self.value

    one = MatmulClass(123)
    two = MatmulClass(234)
    assert one @ two == 28782

    one = Proxy(lambda: MatmulClass(123))
    two = Proxy(lambda: MatmulClass(234))
    assert one @ two == 28782

    one = Proxy(lambda: MatmulClass(123))
    two = MatmulClass(234)
    assert one @ two == 28782

    a = 123
    b = Proxy(lambda: MatmulClass(234))
    assert a @ b == 357

    a = Proxy(lambda: 123)
    b = Proxy(lambda: MatmulClass(234))
    assert a @ b == 357


def test_div() -> None:
    # On Python 2 this will pick up div and on Python
    # 3 it will pick up truediv.

    two = Proxy(lambda: 2)
    three = Proxy(lambda: 3)

    assert two / three == 2 / 3
    assert 2 / three == 2 / 3
    assert two / 3 == 2 / 3


def test_divdiv() -> None:
    two = Proxy(lambda: 2)
    three = Proxy(lambda: 3)

    assert three // two == 3 // 2
    assert 3 // two == 3 // 2
    assert three // 2 == 3 // 2


def test_mod() -> None:
    two = Proxy(lambda: 2)
    three = Proxy(lambda: 3)

    assert three % two == 3 % 2
    assert 3 % two == 3 % 2
    assert three % 2 == 3 % 2


def test_divmod() -> None:
    two = Proxy(lambda: 2)
    three = Proxy(lambda: 3)

    assert divmod(three, two)
    assert divmod(3, two)
    assert divmod(three, 2)


def test_pow() -> None:
    two = Proxy(lambda: 2)
    three = Proxy(lambda: 3)

    assert three**two == pow(3, 2)
    assert 3**two == pow(3, 2)
    assert pow(3, two) == pow(3, 2)
    assert three**2 == pow(3, 2)

    assert pow(three, two) == pow(3, 2)
    assert pow(3, two) == pow(3, 2)
    assert pow(three, 2) == pow(3, 2)
    assert pow(three, 2, 2) == pow(3, 2, 2)


def test_lshift() -> None:
    two = Proxy(lambda: 2)
    three = Proxy(lambda: 3)

    assert three << two == 3 << 2
    assert 3 << two == 3 << 2
    assert three << 2 == 3 << 2


def test_rshift() -> None:
    two = Proxy(lambda: 2)
    three = Proxy(lambda: 3)

    assert three >> two == 3 >> 2
    assert 3 >> two == 3 >> 2
    assert three >> 2 == 3 >> 2


def test_and() -> None:
    two = Proxy(lambda: 2)
    three = Proxy(lambda: 3)

    assert three & two == 3 & 2
    assert 3 & two == 3 & 2
    assert three & 2 == 3 & 2


def test_xor() -> None:
    two = Proxy(lambda: 2)
    three = Proxy(lambda: 3)

    assert three ^ two == 3 ^ 2
    assert 3 ^ two == 3 ^ 2
    assert three ^ 2 == 3 ^ 2


def test_or() -> None:
    two = Proxy(lambda: 2)
    three = Proxy(lambda: 3)

    assert three | two == 3 | 2
    assert 3 | two == 3 | 2
    assert three | 2 == 3 | 2


def test_iadd() -> None:
    value = Proxy(lambda: 1)
    one = Proxy(lambda: 1)

    value += 1
    assert value == 2
    assert type(value) is Proxy

    value += one
    assert value == 3
    assert type(value) is Proxy


def test_isub() -> None:
    value = Proxy(lambda: 1)
    one = Proxy(lambda: 1)

    value -= 1
    assert value == 0
    assert type(value) is Proxy

    value -= one
    assert value == -1
    assert type(value) is Proxy


def test_imul() -> None:
    value = Proxy(lambda: 2)
    two = Proxy(lambda: 2)

    value *= 2
    assert value == 4
    assert type(value) is Proxy

    value *= two
    assert value == 8
    assert type(value) is Proxy


def test_imatmul() -> None:
    class InplaceMatmul:
        value = None

        def __imatmul__(self, other):
            self.value = other
            return self

    value = InplaceMatmul()
    assert value.value is None
    value @= 123
    assert value.value == 123

    value = Proxy(InplaceMatmul)
    value @= 234
    assert value.value == 234
    assert type(value) is Proxy


def test_idiv() -> None:
    # On Python 2 this will pick up div and on Python
    # 3 it will pick up truediv.

    value = Proxy(lambda: 2)
    two = Proxy(lambda: 2)

    value /= 2
    assert value == 2 / 2
    assert type(value) is Proxy

    value /= two
    assert value == 2 / 2 / 2
    assert type(value) is Proxy


def test_ifloordiv() -> None:
    value = Proxy(lambda: 2)
    two = Proxy(lambda: 2)

    value //= 2
    assert value == 2 // 2
    assert type(value) is Proxy

    value //= two
    assert value == 2 // 2 // 2
    assert type(value) is Proxy


def test_imod() -> None:
    value = Proxy(lambda: 10)
    two = Proxy(lambda: 2)

    value %= 2
    assert value == 10 % 2
    assert type(value) is Proxy

    value %= two
    assert value == 10 % 2 % 2
    assert type(value) is Proxy


def test_ipow() -> None:
    value = Proxy(lambda: 10)
    two = Proxy(lambda: 2)

    value **= 2
    assert value == 10**2
    assert type(value) is Proxy

    value **= two
    assert value == 10**2**2
    assert type(value) is Proxy


def test_ilshift() -> None:
    value = Proxy(lambda: 256)
    two = Proxy(lambda: 2)

    value <<= 2
    assert value == 256 << 2
    assert type(value) is Proxy

    value <<= two
    assert value == 256 << 2 << 2
    assert type(value) is Proxy


def test_irshift() -> None:
    value = Proxy(lambda: 2)
    two = Proxy(lambda: 2)

    value >>= 2
    assert value == 2 >> 2
    assert type(value) is Proxy

    value >>= two
    assert value == 2 >> 2 >> 2
    assert type(value) is Proxy


def test_iand() -> None:
    value = Proxy(lambda: 1)
    two = Proxy(lambda: 2)

    value &= 2
    assert value == 1 & 2
    assert type(value) is Proxy

    value &= two
    assert value == 1 & 2 & 2
    assert type(value) is Proxy


def test_ixor() -> None:
    value = Proxy(lambda: 1)
    two = Proxy(lambda: 2)

    value ^= 2
    assert value == 1 ^ 2
    assert type(value) is Proxy

    value ^= two
    assert value == 1 ^ 2 ^ 2
    assert type(value) is Proxy


def test_ior() -> None:
    value = Proxy(lambda: 1)
    two = Proxy(lambda: 2)

    value |= 2
    assert value == 1 | 2
    assert type(value) is Proxy

    value |= two
    assert value == 1 | 2 | 2
    assert type(value) is Proxy


def test_neg() -> None:
    value = Proxy(lambda: 1)

    assert -value == -1


def test_pos() -> None:
    value = Proxy(lambda: 1)

    assert +value == 1


def test_abs() -> None:
    value = Proxy(lambda: -1)

    assert abs(value) == 1


def test_invert() -> None:
    value = Proxy(lambda: 1)

    assert ~value == ~1


def test_decimal_complex() -> None:
    instance = decimal.Decimal(123)

    proxy = Proxy(lambda: instance)

    assert complex(instance) == complex(proxy)


def test_fractions_round() -> None:
    instance = fractions.Fraction('1/2')

    proxy = Proxy(lambda: instance)

    assert round(instance) == round(proxy)
