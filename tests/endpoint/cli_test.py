from __future__ import annotations

import logging
import os
import subprocess
import uuid
from typing import Generator
from unittest import mock

import pytest

import proxystore
from proxystore.endpoint.cli import main
from proxystore.endpoint.config import read_config


@pytest.fixture()
def default_dir(tmp_dir) -> Generator[str, None, None]:
    with mock.patch(
        'proxystore.endpoint.commands.default_dir',
        return_value=tmp_dir,
    ):
        yield tmp_dir


def test_no_args_prints_help(capsys) -> None:
    with pytest.raises(SystemExit):
        main([])
    captured = capsys.readouterr()
    assert captured.out.startswith('usage: proxystore-endpoint [-h]')


def test_help_with_no_command(capsys) -> None:
    with pytest.raises(SystemExit):
        main(['help'])
    captured = capsys.readouterr()
    assert captured.out.startswith('usage: proxystore-endpoint [-h]')


def test_help_with_command(capsys) -> None:
    with pytest.raises(SystemExit):
        main(['help', 'list'])
    captured = capsys.readouterr()
    assert captured.out.startswith('usage: proxystore-endpoint list [-h]')


def test_configure(default_dir) -> None:
    name = 'my-endpoint'
    host = '127.0.0.1'
    port = 4321
    server = 'server:1234'
    main(
        [
            'configure',
            name,
            '--host',
            host,
            '--port',
            str(port),
            '--server',
            server,
        ],
    )

    endpoint_dir = os.path.join(default_dir, name)
    assert os.path.isdir(endpoint_dir)
    cfg = read_config(endpoint_dir)
    assert cfg.name == name
    assert cfg.host == host
    assert cfg.port == port
    assert cfg.server == server


def test_list(default_dir, caplog) -> None:
    # Note: because default_dir is mocked, there's nothing to list so we
    # are really testing that the correct command in
    # proxystore.endpoint.commands is called and leaving the testing of that
    # command to tests/endpoint/commands_test.py.
    caplog.set_level(logging.INFO)
    main(['list'])
    assert len(caplog.records) == 1
    assert 'No valid endpoint configurations' in caplog.records[0].message


def test_remove(default_dir, caplog) -> None:
    # Note: similar to test_list()
    caplog.set_level(logging.ERROR)
    main(['remove', 'my-endpoint'])
    assert len(caplog.records) == 1
    assert any(
        ['does not exist' in record.message for record in caplog.records],
    )


def test_serve(default_dir, caplog) -> None:
    # Note: similar to test_list()
    caplog.set_level(logging.ERROR)
    main(['start', 'my-endpoint'])
    assert len(caplog.records) == 1
    assert any(
        ['does not correspond' in record.message for record in caplog.records],
    )


def test_entry_point() -> None:
    result = subprocess.run(
        ['proxystore-endpoint', '--version'],
        capture_output=True,
    )
    assert result.returncode == 0
    assert proxystore.__version__ in str(result.stdout)

    result = subprocess.run(
        ['proxystore-endpoint', 'list'],
        capture_output=True,
    )
    assert result.returncode == 0

    result = subprocess.run(
        ['proxystore-endpoint', 'remove', str(uuid.uuid4())],
        capture_output=True,
    )
    assert result.returncode == 1
