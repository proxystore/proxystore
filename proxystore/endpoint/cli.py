"""`proxystore-endpoint` command-line interface.

See the CLI Reference for the
[`proxystore-endpoint`](../cli.md#proxystore-endpoint) usage instructions.
"""

from __future__ import annotations

import contextlib
import functools
import logging
import sys
import time
import uuid
from collections.abc import Callable
from collections.abc import Generator
from typing import ClassVar
from typing import Literal
from typing import ParamSpec

import click

import proxystore
from proxystore.endpoint.client import EndpointClient
from proxystore.endpoint.config import EndpointP2PConfig
from proxystore.endpoint.config import EndpointStorageConfig
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.directory import EndpointStatus
from proxystore.endpoint.directory import resolve_home
from proxystore.endpoint.exceptions import EndpointError
from proxystore.endpoint.exceptions import EndpointExistsError
from proxystore.endpoint.exceptions import EndpointNotFoundError
from proxystore.endpoint.exceptions import EndpointRunningError
from proxystore.endpoint.exceptions import PeerExistsError
from proxystore.endpoint.process import start_endpoint
from proxystore.endpoint.process import stop_endpoint
from proxystore.serialize import deserialize
from proxystore.serialize import serialize

logger = logging.getLogger(__name__)

P = ParamSpec('P')


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


def _exit_on_error(func: Callable[P, None]) -> Callable[P, None]:
    """Log endpoint errors and exit with status 1."""

    @functools.wraps(func)
    def _wrapped(*args: P.args, **kwargs: P.kwargs) -> None:
        try:
            func(*args, **kwargs)
        except EndpointNotFoundError as e:
            logger.error(e)
            logger.error('Use `proxystore-endpoint list` to see endpoints.')
            raise SystemExit(1) from None
        except (EndpointError, ValueError) as e:
            logger.error(e)
            raise SystemExit(1) from None

    return _wrapped


def _parse_relays(relays: str) -> Literal['n0', 'none'] | list[str]:
    relays = relays.strip()
    if relays in ('n0', 'none'):
        return relays  # type: ignore[return-value]
    return [url.strip() for url in relays.split(',') if url.strip()]


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
    '--peering/--no-peering',
    default=True,
    metavar='BOOL',
    help='Enable communication with peer endpoints.',
)
@click.option(
    '--relays',
    default='n0',
    metavar='RELAYS',
    help=(
        'Relays used for peering: "n0" (public relays run by n0), "none", '
        'or a comma-separated list of self-hosted relay URLs.'
    ),
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
@_exit_on_error
def configure(
    name: str,
    host: str,
    port: int | None,
    peering: bool,
    relays: str,
    persist: bool,
    tls: bool,
) -> None:
    """Configure a new endpoint."""
    if host.lower().strip() in ('ip', 'fqdn'):
        host = host.lower().strip()
    try:
        endpoint_dir = EndpointDir.create(
            name,
            host=host,
            port=port,
            tls=tls,
            p2p=EndpointP2PConfig(
                enabled=peering,
                relays=_parse_relays(relays),
            ),
            storage=EndpointStorageConfig(
                backend='sqlite' if persist else 'memory',
            ),
        )
    except EndpointExistsError as e:
        logger.error(e)
        logger.info('To reconfigure the endpoint, remove and try again.')
        raise SystemExit(1) from None

    config = endpoint_dir.read_config()
    logger.info('Configured endpoint: %s <%s>', name, config.id)
    logger.info('Config and log file directory: %s', endpoint_dir)
    logger.info('Start the endpoint with:')
    logger.info('  $ proxystore-endpoint start %s', name)
    if peering:
        logger.info('Allow a peer endpoint to communicate with this one with:')
        logger.info(
            '  $ proxystore-endpoint peers add %s PEER_NAME PEER_ID', name
        )


@cli.command(name='list')
def list_all() -> None:
    """List all user endpoints."""
    endpoints = EndpointDir.find_all()
    if len(endpoints) == 0:
        logger.info('No valid endpoint configurations in %s.', resolve_home())
        return

    name_width = max(18, *(len(c.name) for _, c in endpoints))
    status_width = max(len(s.name) for s in EndpointStatus)
    logger.info(
        '%-*s %-*s ID',
        name_width,
        'NAME',
        status_width,
        'STATUS',
        extra={'simple': True},
    )
    logger.info(
        '=' * (name_width + status_width + 2 + len(endpoints[0][1].id)),
        extra={'simple': True},
    )
    for endpoint_dir, config in sorted(endpoints, key=lambda e: e[1].name):
        logger.info(
            '%-*s %-*s %s',
            name_width,
            config.name,
            status_width,
            endpoint_dir.status().name,
            config.id,
            extra={'simple': True},
        )


@cli.command(name='id')
@click.argument('name', metavar='NAME', required=True)
@_exit_on_error
def endpoint_id(name: str) -> None:
    """Print the ID of an endpoint."""
    config = EndpointDir.from_name(name).read_config()
    logger.info(config.id, extra={'simple': True})


@cli.group()
def peers() -> None:
    """Manage the peers an endpoint can communicate with.

    Two endpoints can only communicate if each endpoint has the other in its
    peers. Get the ID of an endpoint with "proxystore-endpoint id NAME".
    """


@peers.command(name='add')
@click.argument('name', metavar='NAME', required=True)
@click.argument('peer_name', metavar='PEER_NAME', required=True)
@click.argument('peer_id', metavar='PEER_ID', required=True)
@_exit_on_error
def peers_add(name: str, peer_name: str, peer_id: str) -> None:
    """Allow endpoint NAME to communicate with peer PEER_ID."""
    endpoint_dir = EndpointDir.from_name(name)
    try:
        added = endpoint_dir.peers.add(peer_name, peer_id)
    except PeerExistsError as e:
        logger.error('%s Remove it first with:', e)
        logger.error(
            '  $ proxystore-endpoint peers remove %s %s',
            name,
            peer_name,
        )
        raise SystemExit(1) from None
    logger.info('Added peer %s <%s> to endpoint %s.', peer_name, added, name)
    logger.info(
        'The peer must also add this endpoint <%s> to its peers.',
        endpoint_dir.read_config().id,
    )


@peers.command(name='remove')
@click.argument('name', metavar='NAME', required=True)
@click.argument('peer_name', metavar='PEER_NAME', required=True)
@_exit_on_error
def peers_remove(name: str, peer_name: str) -> None:
    """Stop endpoint NAME from communicating with peer PEER_NAME."""
    removed = EndpointDir.from_name(name).peers.remove(peer_name)
    logger.info(
        'Removed peer %s <%s> from endpoint %s.',
        peer_name,
        removed,
        name,
    )


@peers.command(name='list')
@click.argument('name', metavar='NAME', required=True)
@_exit_on_error
def peers_list(name: str) -> None:
    """List the peers of endpoint NAME."""
    peers = EndpointDir.from_name(name).peers.read().peers
    if len(peers) == 0:
        logger.info('Endpoint %s has no peers.', name)
        logger.info('Add a peer with:')
        logger.info('  $ proxystore-endpoint peers add %s NAME ID', name)
        return

    name_width = max(len('NAME'), *(len(n) for n in peers))
    logger.info('%-*s ID', name_width, 'NAME', extra={'simple': True})
    logger.info('=' * (name_width + 65), extra={'simple': True})
    for peer_name, peer_id in sorted(peers.items()):
        logger.info(
            '%-*s %s',
            name_width,
            peer_name,
            peer_id,
            extra={'simple': True},
        )


@cli.command()
@click.argument('name', metavar='NAME', required=True)
@_exit_on_error
def remove(name: str) -> None:
    """Remove an endpoint."""
    try:
        EndpointDir.from_name(name).remove()
    except EndpointRunningError as e:
        logger.error(e)
        logger.error('  $ proxystore-endpoint stop %s', name)
        raise SystemExit(1) from None
    logger.info('Removed endpoint named %s.', name)


@cli.command()
@click.argument('name', metavar='NAME', required=True)
@click.option('--detach/--no-detach', default=True, help='Run as daemon.')
@click.pass_context
@_exit_on_error
def start(ctx: click.Context, name: str, detach: bool) -> None:
    """Start an endpoint."""
    start_endpoint(
        EndpointDir.from_name(name),
        detach=detach,
        log_level=ctx.obj['LOG_LEVEL'],
    )


@cli.command()
@click.argument('name', metavar='NAME', required=True)
@_exit_on_error
def stop(name: str) -> None:
    """Stop a detached endpoint."""
    if stop_endpoint(EndpointDir.from_name(name)):
        logger.info('Endpoint %s has been stopped.', name)
    else:
        logger.info('Endpoint %s is not running.', name)


@cli.group(name='client')
@click.argument('name', metavar='NAME', required=True)
@click.option(
    '--target',
    metavar='ID',
    help='Optional ID of a peer endpoint to forward operations to.',
)
@click.pass_context
def client_group(
    ctx: click.Context,
    name: str,
    target: str | None,
) -> None:
    """Run client operations on endpoint NAME.

    Operations are performed on the endpoint or, with --target, forwarded
    to a peer endpoint. These are useful for testing and debugging endpoints.
    """
    ctx.ensure_object(dict)

    ctx.obj['ENDPOINT_NAME'] = name
    ctx.obj['TARGET_ENDPOINT_ID'] = target


@contextlib.contextmanager
def _endpoint_client(
    ctx: click.Context,
) -> Generator[EndpointClient, None, None]:
    """Connect to the endpoint of a client command and handle errors."""
    try:
        with EndpointClient.from_name(ctx.obj['ENDPOINT_NAME']) as client:
            yield client
    except (EndpointError, ValueError) as e:
        logger.error(e)
        raise SystemExit(1) from None


@client_group.command()
@click.argument('key', metavar='KEY', required=True)
@click.pass_context
def evict(ctx: click.Context, key: str) -> None:
    """Evict object from an endpoint."""
    with _endpoint_client(ctx) as client:
        client.evict(key, ctx.obj['TARGET_ENDPOINT_ID'])
    logger.info('Evicted object from endpoint.')


@client_group.command()
@click.argument('key', metavar='KEY', required=True)
@click.pass_context
def exists(ctx: click.Context, key: str) -> None:
    """Check if object exists in an endpoint."""
    with _endpoint_client(ctx) as client:
        res = client.exists(key, ctx.obj['TARGET_ENDPOINT_ID'])
    logger.info('Object exists: %s', res)


@client_group.command()
@click.argument('key', metavar='KEY', required=True)
@click.pass_context
def get(ctx: click.Context, key: str) -> None:
    """Get an object from an endpoint."""
    with _endpoint_client(ctx) as client:
        res = client.get(key, ctx.obj['TARGET_ENDPOINT_ID'])

    if res is None:
        logger.info('Object does not exist.')
    else:
        obj = deserialize(res)
        logger.info('Result: %s', obj)


@client_group.command()
@click.option(
    '--count',
    default=4,
    type=click.IntRange(min=1),
    metavar='COUNT',
    help='Number of pings to send.',
)
@click.option(
    '--interval',
    default=0.5,
    type=click.FloatRange(min=0),
    metavar='SECONDS',
    help='Seconds to wait between pings.',
)
@click.pass_context
def ping(ctx: click.Context, count: int, interval: float) -> None:
    """Measure the latency of and path to a peer endpoint.

    The endpoint sends each ping to the peer given by --target and reports
    the time until it received the response and if the connection is direct
    or relayed. The first ping includes the time to connect to the peer if
    the endpoint is not already connected. Without --target, the time is the
    round trip between this client and the endpoint.
    """
    target = ctx.obj['TARGET_ENDPOINT_ID']
    times: list[float] = []
    with _endpoint_client(ctx) as client:
        for i in range(count):
            if i > 0:
                time.sleep(interval)
            start = time.perf_counter()
            try:
                result = client.ping(target)
            except (EndpointError, ValueError) as e:
                logger.error(e)
                raise SystemExit(1) from None
            client_ms = (time.perf_counter() - start) * 1000

            if result.peer_rtt_ms is None:
                times.append(client_ms)
                logger.info(
                    'Reply from local endpoint: time=%.2f ms',
                    client_ms,
                )
                continue

            times.append(result.peer_rtt_ms)
            if result.relayed is None:  # pragma: no cover
                path = 'unknown'
            else:
                kind = 'relayed via' if result.relayed else 'direct to'
                path = (
                    f'{kind} {result.remote_addr} '
                    f'(rtt {result.path_rtt_ms} ms)'
                )
            logger.info(
                'Reply from %s: time=%.2f ms path=%s',
                target,
                result.peer_rtt_ms,
                path,
            )

    logger.info(
        '%d ping(s): min/avg/max = %.2f/%.2f/%.2f ms',
        len(times),
        min(times),
        sum(times) / len(times),
        max(times),
    )


@client_group.command()
@click.argument('data', required=True)
@click.pass_context
def put(ctx: click.Context, data: str) -> None:
    """Put an object in an endpoint."""
    key = str(uuid.uuid4())
    with _endpoint_client(ctx) as client:
        client.set(key, serialize(data), ctx.obj['TARGET_ENDPOINT_ID'])
    logger.info('Put object in endpoint with key %s', key)
