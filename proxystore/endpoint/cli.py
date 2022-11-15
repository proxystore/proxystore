""":code:`proxystore-endpoint` command-line interface."""
from __future__ import annotations

import argparse
import logging
import sys
from typing import Sequence

import proxystore
from proxystore.endpoint.commands import configure_endpoint
from proxystore.endpoint.commands import list_endpoints
from proxystore.endpoint.commands import remove_endpoint
from proxystore.endpoint.commands import start_endpoint
from proxystore.endpoint.commands import stop_endpoint


class _CLIFormatter(logging.Formatter):
    """Custom format for CLI printing.

    Source: https://stackoverflow.com/questions/1343227
    """

    grey = '\x1b[0;30m'
    red = '\x1b[0;31m'
    green = '\x1b[0;32m'
    yellow = '\x1b[0;33m'
    cyan = '\x1b[0;36m'
    bold_red = '\x1b[1;31m'
    reset = '\x1b[0m'

    FORMATS = {
        logging.DEBUG: f'{cyan}DEBUG:{reset} %(message)s',
        logging.INFO: f'{green}INFO:{reset} %(message)s',
        logging.WARNING: f'{yellow}WARNING:{reset} %(message)s',
        logging.ERROR: f'{red}ERROR:{reset} %(message)s',
        logging.CRITICAL: f'{bold_red}CRITICAL:{reset} %(message)s',
    }

    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover
        if hasattr(record, 'simple') and record.simple:
            return record.getMessage()
        else:
            formatter = logging.Formatter(self.FORMATS[record.levelno])
            return formatter.format(record)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI for managing endpoints on the local system.

    Usage:

    .. code-block:: console

       $ proxystore-endpoint [command] {options}
       $ proxystore-endpoint --help
    """
    argv = argv if argv is not None else sys.argv[1:]
    parser = argparse.ArgumentParser(
        prog='proxystore-endpoint',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # https://stackoverflow.com/a/8521644/812183
    parser.add_argument(
        '-V',
        '--version',
        action='version',
        version=f'%(prog)s {proxystore.__version__}',
    )
    parser.add_argument(
        '--log-level',
        choices=['ERROR', 'WARNING', 'INFO', 'DEBUG'],
        default='INFO',
        help='logging level for CLI and any subprocesses',
    )
    subparsers = parser.add_subparsers(dest='command')

    # Command: configure
    parser_configure = subparsers.add_parser(
        'configure',
        help='configure a new endpoint',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_configure.add_argument('name', help='name of endpoint')
    parser_configure.add_argument(
        '--port',
        type=int,
        default=9753,
        help='port the endpoint should listen on',
    )
    parser_configure.add_argument(
        '--server',
        default=None,
        help='signaling server address for P2P connections',
    )
    parser_configure.add_argument(
        '--max-memory',
        default=None,
        type=int,
        required='--dump-dir' in sys.argv,
        help='optional maximum number of bytes to store in memory',
    )
    parser_configure.add_argument(
        '--dump-dir',
        default=None,
        required='--max-memory' in sys.argv,
        help='optional directory to dump objects to if max_memory reached',
    )
    parser_configure.add_argument(
        '--peer-channels',
        default=1,
        type=int,
        help='datachannels per peer connection',
    )

    # Command: list
    subparsers.add_parser(
        'list',
        help='list all user endpoints',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Command: remove
    parser_remove = subparsers.add_parser(
        'remove',
        help='remove an endpoint',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_remove.add_argument('name', help='name of endpoint')

    # Command: start
    parser_start = subparsers.add_parser(
        'start',
        help='start an endpoint',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_start.add_argument('name', help='name of endpoint')
    parser_start.add_argument(
        '--no-detach',
        action='store_true',
        help='do not detach the endpoint process',
    )

    # Command: stop
    parser_stop = subparsers.add_parser(
        'stop',
        help='stop an endpoint',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_stop.add_argument('name', help='name of endpoint')

    # Source: https://github.com/pre-commit/pre-commit
    parser_help = subparsers.add_parser(
        'help',
        help='show help for a specific command',
    )
    parser_help.add_argument(
        'help_command',
        nargs='?',
        help='command to show help for',
    )

    if len(argv) == 0:
        argv = ['--help']

    # https://stackoverflow.com/questions/46962065
    known, unknown = parser.parse_known_args(argv)
    args = parser.parse_args(unknown, namespace=known)

    if args.command == 'help' and args.help_command is not None:
        parser.parse_args([args.help_command, '--help'])
    elif args.command == 'help':
        parser.parse_args(['--help'])

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_CLIFormatter())
    logging.basicConfig(level=args.log_level, handlers=[handler])

    if args.command == 'configure':
        return configure_endpoint(
            args.name,
            port=args.port,
            server=args.server,
            max_memory=args.max_memory,
            dump_dir=args.dump_dir,
        )
    elif args.command == 'list':
        return list_endpoints()
    elif args.command == 'remove':
        return remove_endpoint(args.name)
    elif args.command == 'start':
        return start_endpoint(
            args.name,
            detach=not args.no_detach,
            log_level=args.log_level,
        )
    elif args.command == 'stop':
        return stop_endpoint(args.name)
    else:
        raise NotImplementedError(
            f'{args.command} is not a supported command. '
            'Use --help for list of commands.',
        )

    raise AssertionError(f'{args.command} failed to exit with a return code.')


if __name__ == '__main__':
    raise SystemExit(main())
