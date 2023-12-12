from __future__ import annotations

import logging
from unittest import mock

import pytest
import stun

from proxystore.p2p.nat import check_nat
from proxystore.p2p.nat import check_nat_and_log
from proxystore.p2p.nat import NatType
from proxystore.p2p.nat import Result
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


def test_check_nat_and_log_normal(caplog) -> None:
    caplog.set_level(logging.INFO)

    result = Result(NatType.RestrictedCone, '192.168.1.1', 1234)
    with mock.patch('proxystore.p2p.nat.check_nat', return_value=result):
        check_nat_and_log()

    assert any(
        [
            'NAT Type:       Restricted-cone NAT' in r.message
            for r in caplog.records
        ],
    )
    assert any(
        ['External IP:    192.168.1.1' in r.message for r in caplog.records],
    )
    assert any(['External Port:  1234' in r.message for r in caplog.records])
    assert any(
        [
            r.message.startswith(
                'NAT traversal for peer-to-peer methods (e.g., hole-punching) '
                'is likely to work.',
            )
            for r in caplog.records
        ],
    )


def test_check_nat_and_log_symmetric(caplog) -> None:
    caplog.set_level(logging.INFO)

    result = Result(NatType.Symmetric, '192.168.1.1', 1234)
    with mock.patch('proxystore.p2p.nat.check_nat', return_value=result):
        check_nat_and_log()

    assert any(
        ['NAT Type:       Symmetric NAT' in r.message for r in caplog.records],
    )
    assert any(
        ['External IP:    192.168.1.1' in r.message for r in caplog.records],
    )
    assert any(['External Port:  1234' in r.message for r in caplog.records])
    assert any(
        [
            r.message.startswith(
                'NAT traversal (e.g., hole-punching) does not work '
                'reliably across',
            )
            for r in caplog.records
        ],
    )


def test_check_nat_and_log_failure(caplog) -> None:
    caplog.set_level(logging.INFO)

    with mock.patch(
        'proxystore.p2p.nat.check_nat',
        side_effect=RuntimeError('test error'),
    ):
        check_nat_and_log()

    assert any(
        [
            r.message.startswith('Failed to determine NAT type: test error')
            for r in caplog.records
        ],
    )
