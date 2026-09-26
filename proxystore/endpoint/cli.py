"""`proxystore-endpoint` command-line interface.

See the CLI Reference for the
[`proxystore-endpoint`](../cli.md#proxystore-endpoint) usage instructions.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
import uuid
from collections.abc import Generator
from typing import ClassVar

import click

import proxystore
from proxystore.endpoint.client import EndpointClient
from proxystore.endpoint.commands import configure_endpoint
from proxystore.endpoint.commands import list_endpoints
from proxystore.endpoint.commands import remove_endpoint
from proxystore.endpoint.commands import start_endpoint
from proxystore.endpoint.commands import stop_endpoint
from proxystore.endpoint.exceptions import EndpointError
from proxystore.p2p.nat import check_nat_and_log
from proxystore.serialize import deserialize
from proxystore.serialize import serialize

logger = logging.getLogger(__name__)


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

    FORMATS: ClassVar[dict[int, str]] = {
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


@cli.command(name='check-nat')
@click.option(
    '--host',
    default='0.0.0.0',
    metavar='ADDR',
    help='Network interface address to listen on.',
)
@click.option(
    '--port',
    default=0,
    type=int,
    metavar='PORT',
    help='Port to listen on. Defaults to an ephemeral port.',
)
def check_nat_command(host: str, port: int) -> None:
    """Check the NAT mapping behavior of your network."""
    asyncio.run(check_nat_and_log(host, port))


@cli.command()
@click.argument('name', metavar='NAME', required=True)
@click.option(
    '--host',
    default='ip',
    type=str,
    help='Set endpoint host using the "ip", "fqdn", or a static address.',
)
@click.option(
    '--port',
    default=None,
    type=int,
    metavar='PORT',
    help='Port to listen on.',
)
@click.option(
    '--relay-address',
    default='wss://relay.proxystore.dev',
    metavar='ADDR',
    help='Relay server address.',
)
@click.option(
    '--relay-auth/--no-relay-auth',
    default=True,
    metavar='BOOL',
    help='Disable relay server authentication.',
)
@click.option(
    '--relay-server/--no-relay-server',
    default=True,
    metavar='BOOL',
    help='Disable connecting to the relay server on start.',
)
@click.option(
    '--peer-channels',
    default=1,
    type=int,
    metavar='COUNT',
    help='Datachannels to use per peer connection.',
)
@click.option(
    '--persist/--no-persist',
    default=False,
    metavar='BOOL',
    help='Optionally persist data to a database.',
)
@click.option(
    '--tls/--no-tls',
    default=False,
    metavar='BOOL',
    help='Encrypt connections from clients with TLS.',
)
def configure(
    name: str,
    host: str,
    port: int | None,
    relay_address: str,
    relay_auth: bool,
    relay_server: bool,
    peer_channels: int,
    persist: bool,
    tls: bool,
) -> None:
    """Configure a new endpoint."""
    raise SystemExit(
        configure_endpoint(
            name,
            host=host,
            peer_channels=peer_channels,
            persist_data=persist,
            port=port,
            relay_auth=relay_auth,
            tls=tls,
            relay_server=relay_address if relay_server else None,
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


@cli.group()
@click.argument('name', metavar='NAME', required=True)
@click.option(
    '--remote',
    metavar='UUID',
    help='Optional UUID of remote endpoint to use.',
)
@click.pass_context
def test(
    ctx: click.Context,
    name: str,
    remote: str | None,
) -> None:
    """Execute test commands on an endpoint."""
    ctx.ensure_object(dict)

    ctx.obj['ENDPOINT_NAME'] = name
    ctx.obj['REMOTE_ENDPOINT_UUID'] = remote


@contextlib.contextmanager
def _endpoint_client(
    ctx: click.Context,
) -> Generator[EndpointClient, None, None]:
    """Connect to the endpoint of a test command and handle errors."""
    try:
        with EndpointClient.from_name(ctx.obj['ENDPOINT_NAME']) as client:
            yield client
    except (EndpointError, ValueError) as e:
        logger.error(e)
        raise SystemExit(1) from None


@test.command()
@click.argument('key', metavar='KEY', required=True)
@click.pass_context
def evict(ctx: click.Context, key: str) -> None:
    """Evict object from an endpoint."""
    with _endpoint_client(ctx) as client:
        client.evict(key, ctx.obj['REMOTE_ENDPOINT_UUID'])
    logger.info('Evicted object from endpoint.')


@test.command()
@click.argument('key', metavar='KEY', required=True)
@click.pass_context
def exists(ctx: click.Context, key: str) -> None:
    """Check if object exists in an endpoint."""
    with _endpoint_client(ctx) as client:
        res = client.exists(key, ctx.obj['REMOTE_ENDPOINT_UUID'])
    logger.info(f'Object exists: {res}')


@test.command()
@click.argument('key', metavar='KEY', required=True)
@click.pass_context
def get(ctx: click.Context, key: str) -> None:
    """Get an object from an endpoint."""
    with _endpoint_client(ctx) as client:
        res = client.get(key, ctx.obj['REMOTE_ENDPOINT_UUID'])

    if res is None:
        logger.info('Object does not exist.')
    else:
        obj = deserialize(res)
        logger.info(f'Result: {obj}')


@test.command()
@click.argument('data', required=True)
@click.pass_context
def put(ctx: click.Context, data: str) -> None:
    """Put an object in an endpoint."""
    key = str(uuid.uuid4())
    with _endpoint_client(ctx) as client:
        client.set(key, serialize(data), ctx.obj['REMOTE_ENDPOINT_UUID'])
    logger.info(f'Put object in endpoint with key {key}')
