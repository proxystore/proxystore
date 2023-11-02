from __future__ import annotations

from unittest import mock

import pytest
import stun

from proxystore.p2p.nat import check_nat
from proxystore.p2p.nat import NatType
from testing.utils import open_port


def test_nat_type() -> None:
    assert NatType._from_str(stun.FullCone) == NatType.FullCone

    with pytest.raises(RuntimeError, match='No response from a STUN server.'):
        NatType._from_str(stun.Blocked)

    with pytest.raises(
        RuntimeError,
        match='Address changed during NAT type check.',
    ):
        NatType._from_str(stun.ChangedAddressError)


def test_check_nat() -> None:
    external = {'ExternalIP': '192.168.1.1', 'ExternalPort': 1234}

    with mock.patch(
        'stun.get_nat_type',
        return_value=(stun.RestricNAT, external),
    ):
        result = check_nat(source_port=open_port())

    assert result.nat_type == NatType.RestrictedCone
    assert result.external_ip == external['ExternalIP']
    assert result.external_port == external['ExternalPort']


def test_check_nat_failure() -> None:
    external = {'ExternalIP': '192.168.1.1', 'ExternalPort': 1234}

    with mock.patch(
        'stun.get_nat_type',
        return_value=(stun.Blocked, external),
    ):
        with pytest.raises(RuntimeError, match='No STUN servers returned'):
            check_nat(source_port=open_port())
