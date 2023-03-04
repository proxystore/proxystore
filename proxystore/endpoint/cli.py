"""`proxystore-endpoint` command-line interface.

See the CLI Reference for the
[`proxystore-endpoint`](../cli.md#proxystore-endpoint) usage instructions.
"""
from __future__ import annotations

import logging
import sys

import click

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


@click.group()
@click.option(
    '--log-level',
    default='INFO',
    type=click.Choice(
        ['ERROR', 'WARNING', 'INFO', 'DEBUG'],
        case_sensitive=False,
    ),
    help='Minimum logging level.',
)
@click.pass_context
def cli(ctx: click.Context, log_level: str) -> None:
    """Manage and start ProxyStore Endpoints."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_CLIFormatter())
    logging.basicConfig(level=log_level, handlers=[handler])
    ctx.ensure_object(dict)
    ctx.obj['LOG_LEVEL'] = log_level


@cli.command(name='help')
def show_help() -> None:
    """Show available commands and options."""
    with click.Context(cli) as ctx:
        click.echo(cli.get_help(ctx))


@cli.command()
def version() -> None:
    """Show the ProxyStore version."""
    click.echo(f'ProxyStore v{proxystore.__version__}')


@cli.command()
@click.argument('name', metavar='NAME', required=True)
@click.option(
    '--port',
    default=8765,
    type=int,
    metavar='PORT',
    help='Port to listen on.',
)
@click.option(
    '--server',
    default=None,
    metavar='ADDR',
    help='Optional signaling server address.',
)
@click.option(
    '--max-memory',
    default=None,
    type=int,
    metavar='BYTES',
    help='Optional maximum memory to use.',
)
@click.option(
    '--dump-dir',
    default=None,
    metavar='PATH',
    help='Directory to dump object to if max-memory exceeded.',
)
@click.option(
    '--peer-channels',
    default=1,
    type=int,
    metavar='COUNT',
    help='Datachannels to use per peer connection',
)
def configure(
    name: str,
    port: int,
    server: str,
    max_memory: int | None,
    dump_dir: str | None,
    peer_channels: int,
) -> None:
    """Configure a new endpoint."""
    raise SystemExit(
        configure_endpoint(
            name,
            port=port,
            server=server,
            max_memory=max_memory,
            dump_dir=dump_dir,
            peer_channels=peer_channels,
        ),
    )


@cli.command(name='list')
def list_all() -> None:
    """List all user endpoints."""
    raise SystemExit(list_endpoints())


@cli.command()
@click.argument('name', metavar='NAME', required=True)
def remove(name: str) -> None:
    """Remove an endpoint."""
    raise SystemExit(remove_endpoint(name))


@cli.command()
@click.argument('name', metavar='NAME', required=True)
@click.option('--detach/--no-detach', default=True, help='Run as daemon.')
@click.pass_context
def start(ctx: click.Context, name: str, detach: bool) -> None:
    """Start an endpoint."""
    raise SystemExit(
        start_endpoint(name, detach=detach, log_level=ctx.obj['LOG_LEVEL']),
    )


@cli.command()
@click.argument('name', metavar='NAME', required=True)
def stop(name: str) -> None:
    """Stop a detached endpoint."""
    raise SystemExit(stop_endpoint(name))
