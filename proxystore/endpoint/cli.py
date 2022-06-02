"""ProxyStore Endpoint CLI."""
from __future__ import annotations

import argparse
import socket
import sys
from typing import Sequence

import proxystore
from proxystore.endpoint.commands import configure_endpoint
from proxystore.endpoint.commands import list_endpoints
from proxystore.endpoint.commands import remove_endpoint
from proxystore.endpoint.commands import start_endpoint


def main(argv: Sequence[str] | None = None) -> int:
    """CLI for starting an endpoint."""
    argv = argv if argv is not None else sys.argv[1:]
    parser = argparse.ArgumentParser(prog='proxystore-endpoint')

    # https://stackoverflow.com/a/8521644/812183
    parser.add_argument(
        '-V',
        '--version',
        action='version',
        version=f'%(prog)s {proxystore.__version__}',
    )
    subparsers = parser.add_subparsers(dest='command')

    # Command: configure
    parser_configure = subparsers.add_parser(
        'configure',
        help='configure a new endpoint',
    )
    parser_configure.add_argument('name', help='name of endpoint')
    parser_configure.add_argument(
        '--host',
        default=None,
        help=(
            'IP address of host that the endpoint will be run on '
            '(default is IP address of current host)'
        ),
    )
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

    # Command: list
    subparsers.add_parser('list', help='list all user endpoints')

    # Command: remove
    parser_remove = subparsers.add_parser('remove', help='remove an endpoint')
    parser_remove.add_argument('name', help='name of endpoint')

    # Command: start
    parser_start = subparsers.add_parser('start', help='start an endpoint')
    parser_start.add_argument('name', help='name of endpoint')
    parser_start.add_argument(
        '--log-level',
        choices=['ERROR', 'WARNING', 'INFO', 'DEBUG'],
        default='INFO',
        help='minimum logging level',
    )

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
    args = parser.parse_args(argv)

    if args.command == 'help' and args.help_command is not None:
        parser.parse_args([args.help_command, '--help'])
    elif args.command == 'help':
        parser.parse_args(['--help'])

    if args.command == 'configure':
        host = (
            socket.gethostbyname(socket.gethostname())
            if args.host is None
            else args.host
        )
        return configure_endpoint(
            args.name,
            host=host,
            port=args.port,
            server=args.server,
        )
    elif args.command == 'list':
        return list_endpoints()
    elif args.command == 'remove':
        return remove_endpoint(args.name)
    elif args.command == 'start':
        return start_endpoint(args.name, log_level=args.log_level)
    else:
        raise NotImplementedError(
            f'{args.command} is not a supported command. '
            'Use --help for list of commands.',
        )

    raise AssertionError(f'{args.command} failed to exit with a return code.')


if __name__ == '__main__':
    raise SystemExit(main())
