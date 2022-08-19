"""CLI tool to verify all versions in files are correct."""
from __future__ import annotations

import argparse
import re
import sys
from typing import Sequence

PEP440_VERSION = (
    r'\d+\.\d+\.\d+'
    r'(((a|b|rc)[1-9][0-9]*)|(\.post[1-9][0-9]*)|(\.dev[1-9][0-9]*))?'
)
PEP440_VERION_NO_ADDITIONAL = r'^\d+\.\d+.\d+$'

PACKAGE_INIT_FILE = 'proxystore/__init__.py'
PACKAGE_INIT_RE = fr'^__version__ = \'({PEP440_VERSION})\'$'

PACKAGE_SETUP_FILE = 'setup.cfg'
PACKAGE_SETUP_RE = fr'^version = ({PEP440_VERSION})$'

CHANGELOG_FILE = 'docs/changelog.rst'
CHANGELOG_RE = fr'^Version ({PEP440_VERSION})$'


def match_version(
    filepath: str,
    pattern: str,
    version: str,
    only_one: bool = True,
) -> None:
    """Search file for pattern and verify pattern contains correct version.

    Args:
        filepath (str): filepath of file to search.
        pattern (str): regular expression that matches the entire line
            containing the version. Group 1 of the regex should exactly
            match the version and is what will be used to compare to `version`.
        version (str): the version to match against.
        only_one (str): by default, an error will be raised if multiple lines
            are found containing a version.

    Raises:
        ValueError:
            if a match is not found.
    """
    pattern_ = re.compile(pattern)
    matches = []
    with open(filepath) as f:
        for line in f.readlines():
            match = pattern_.match(line)
            if match:
                matches.append(match)

    if len(matches) == 0:
        raise ValueError(f'Did not find version in {filepath}')
    elif only_one and len(matches) > 1:
        raise ValueError(f'Found multiple versions in {filepath}')

    found = matches[0].group(1)
    if found == version:
        print(f'{filepath} version matches {version}')
    else:
        raise ValueError(
            f'Version {found} in {filepath} does not match {version}',
        )


def main(argv: Sequence[str] | None = None) -> int:
    """Version check entrypoint."""
    argv = argv if argv is None else sys.argv

    parser = argparse.ArgumentParser(
        prog='python version_check.py',
        description='Verify versions in files match',
    )
    parser.add_argument(
        'version',
        help='Version to match in files',
    )
    args = parser.parse_args(argv)

    version = args.version
    if version.lower().startswith('v'):
        version = version[1:]

    if not re.match(PEP440_VERSION, version):
        # This is slightly misleading as we also require major.minor.patch
        # notation and that pre/post/dev-release segments start at 1
        print(f'FAILED: version {version} does not match PEP440')
        return 1

    failed = False
    try:
        match_version(PACKAGE_INIT_FILE, PACKAGE_INIT_RE, version)
    except ValueError as e:
        print(f'FAILED: {e}')
        failed |= True

    try:
        match_version(PACKAGE_SETUP_FILE, PACKAGE_SETUP_RE, version)
    except ValueError as e:
        print(f'FAILED: {e}')
        failed |= True

    if re.match(PEP440_VERION_NO_ADDITIONAL, version):
        try:
            match_version(CHANGELOG_FILE, CHANGELOG_RE, version, False)
        except ValueError as e:
            print(f'FAILED: {e}')
        failed |= True

    if failed:
        return 1
    else:
        return 0


if __name__ == '__main__':
    raise SystemExit(main())
