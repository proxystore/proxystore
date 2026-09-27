"""`proxystore-endpoint` command-line interface.

See the CLI Reference for the
[`proxystore-endpoint`](../cli.md#proxystore-endpoint) usage instructions.

The results of commands are printed to stdout and errors are printed to
stderr. The `--log-level` option only controls the logs of ProxyStore (e.g.,
of an endpoint started with `--no-detach`).
"""

from __future__ import annotations

import functools
import logging
import time
import uuid
from collections.abc import Callable
from typing import Literal
from typing import ParamSpec

import click

import proxystore
from proxystore.endpoint.client import EndpointClient
from proxystore.endpoint.config import EndpointP2PConfig
from proxystore.endpoint.config import EndpointStorageConfig
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.directory import EndpointStatus
from proxystore.endpoint.exceptions import EndpointError
from proxystore.endpoint.exceptions import EndpointExistsError
from proxystore.endpoint.exceptions import EndpointNotFoundError
from proxystore.endpoint.exceptions import EndpointRunningError
from proxystore.endpoint.exceptions import PeerExistsError
from proxystore.endpoint.process import start_endpoint
from proxystore.endpoint.process import stop_endpoint
from proxystore.serialize import deserialize
from proxystore.serialize import serialize
from proxystore.utils.environment import home_dir

P = ParamSpec('P')


_STATUS_COLORS = {
    EndpointStatus.RUNNING: 'green',
    EndpointStatus.STOPPED: 'yellow',
    EndpointStatus.STALE: 'red',
    EndpointStatus.OTHER_HOST: 'blue',
}


def _error(message: object) -> None:
    """Print an error message to stderr."""
    error = click.style('Error:', fg='red', bold=True)
    click.echo(f'{error} {message}', err=True)


def _success(message: str) -> None:
    """Print the message of a successful operation."""
    click.secho(message, fg='green')


def _note(message: str) -> None:
    """Print a message about an operation that had no effect."""
    click.secho(message, fg='yellow')


def _command(command: str, *, err: bool = False) -> None:
    """Print an example command for the user to run."""
    click.secho(f'  $ {command}', fg='cyan', err=err)


def _header(header: str, width: int) -> None:
    """Print the header row of a table."""
    click.secho(header, bold=True)
    click.secho('=' * width, dim=True)


@click.group()
@click.option(
    '--log-level',
    default='INFO',
    type=click.Choice(
        ['ERROR', 'WARNING', 'INFO', 'DEBUG'],
        case_sensitive=False,
    ),
    help='Minimum level of ProxyStore logs.',
)
@click.pass_context
def cli(ctx: click.Context, log_level: str) -> None:
    """Manage and start ProxyStore Endpoints."""
    logging.basicConfig(level=log_level, format='%(levelname)s: %(message)s')
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
    """Print endpoint errors and exit with status 1."""

    @functools.wraps(func)
    def _wrapped(*args: P.args, **kwargs: P.kwargs) -> None:
        try:
            func(*args, **kwargs)
        except EndpointNotFoundError as e:
            _error(e)
            _error('See endpoints with:')
            _command('proxystore-endpoint list', err=True)
            raise SystemExit(1) from None
        except (EndpointError, ValueError) as e:
            _error(e)
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
    '--discovery',
    default='n0',
    type=click.Choice(['n0', 'none']),
    help=(
        'Service used to find the addresses of peers: "n0" (public DNS '
        'discovery run by n0) or "none".'
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
    discovery: Literal['n0', 'none'],
    persist: bool,
    tls: bool,
) -> None:
    """Configure a new endpoint."""
    try:
        endpoint_dir = EndpointDir.create(
            name,
            host=host,
            port=port,
            tls=tls,
            p2p=EndpointP2PConfig(
                enabled=peering,
                relays=_parse_relays(relays),
                discovery=discovery,
            ),
            storage=EndpointStorageConfig(
                backend='sqlite' if persist else 'memory',
            ),
        )
    except EndpointExistsError as e:
        _error(e)
        _error('To reconfigure the endpoint, remove and try again.')
        raise SystemExit(1) from None

    config = endpoint_dir.read_config()
    _success(f'Configured endpoint: {name} <{config.id}>')
    click.echo(f'Config and log file directory: {endpoint_dir}')
    click.echo('Start the endpoint with:')
    _command(f'proxystore-endpoint start {name}')
    if peering:
        click.echo('Allow a peer endpoint to communicate with this one with:')
        _command(f'proxystore-endpoint peers add {name} PEER_NAME PEER_ID')


@cli.command(name='list')
def list_all() -> None:
    """List all user endpoints."""
    endpoints = EndpointDir.find_all()
    if len(endpoints) == 0:
        _note(f'No valid endpoint configurations in {home_dir()}.')
        return

    name_width = max(18, *(len(c.name) for _, c in endpoints))
    status_width = max(len(s.name) for s in EndpointStatus)
    _header(
        f'{"NAME":<{name_width}} {"STATUS":<{status_width}} ID',
        name_width + status_width + 2 + len(endpoints[0][1].id),
    )
    for endpoint_dir, config in sorted(endpoints, key=lambda e: e[1].name):
        try:
            status = endpoint_dir.status()
        except EndpointNotFoundError:
            # The endpoint was removed since it was found.
            continue
        # Pad before styling so the ANSI codes do not affect alignment.
        status_str = click.style(
            f'{status.name:<{status_width}}',
            fg=_STATUS_COLORS[status],
        )
        click.echo(f'{config.name:<{name_width}} {status_str} {config.id}')


@cli.command(name='id')
@click.argument('name', metavar='NAME', required=True)
@_exit_on_error
def endpoint_id(name: str) -> None:
    """Print the ID of an endpoint."""
    config = EndpointDir.from_name(name).read_config()
    click.echo(config.id)


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
    peers = EndpointDir.from_name(name).peers()
    try:
        added = peers.add(peer_name, peer_id)
    except PeerExistsError as e:
        _error(f'{e} Remove it first with:')
        _command(
            f'proxystore-endpoint peers remove {name} {peer_name}',
            err=True,
        )
        raise SystemExit(1) from None
    _success(f'Added peer {peer_name} <{added}> to endpoint {name}.')
    click.echo(
        'The peer must also add this endpoint '
        f'<{peers.owner_id}> to its peers.',
    )


@peers.command(name='remove')
@click.argument('name', metavar='NAME', required=True)
@click.argument('peer_name', metavar='PEER_NAME', required=True)
@_exit_on_error
def peers_remove(name: str, peer_name: str) -> None:
    """Stop endpoint NAME from communicating with peer PEER_NAME."""
    removed = EndpointDir.from_name(name).peers().remove(peer_name)
    _success(f'Removed peer {peer_name} <{removed}> from endpoint {name}.')


@peers.command(name='list')
@click.argument('name', metavar='NAME', required=True)
@_exit_on_error
def peers_list(name: str) -> None:
    """List the peers of endpoint NAME."""
    peers = EndpointDir.from_name(name).peers().read().peers
    if len(peers) == 0:
        _note(f'Endpoint {name} has no peers.')
        click.echo('Add a peer with:')
        _command(f'proxystore-endpoint peers add {name} NAME ID')
        return

    name_width = max(len('NAME'), *(len(n) for n in peers))
    _header(f'{"NAME":<{name_width}} ID', name_width + 65)
    for peer_name, peer_id in sorted(peers.items()):
        click.echo(f'{peer_name:<{name_width}} {peer_id}')


@cli.command()
@click.argument('name', metavar='NAME', required=True)
@_exit_on_error
def remove(name: str) -> None:
    """Remove an endpoint."""
    try:
        EndpointDir.from_name(name).remove()
    except EndpointRunningError as e:
        _error(e)
        _command(f'proxystore-endpoint stop {name}', err=True)
        raise SystemExit(1) from None
    _success(f'Removed endpoint named {name}.')


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
    """Stop an endpoint running on this host."""
    if stop_endpoint(EndpointDir.from_name(name)):
        _success(f'Endpoint {name} has been stopped.')
    else:
        _note(f'Endpoint {name} is not running.')


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


def _endpoint_client(ctx: click.Context) -> EndpointClient:
    """Connect to the endpoint of a client command."""
    return EndpointClient.from_name(ctx.obj['ENDPOINT_NAME'])


@client_group.command()
@click.argument('key', metavar='KEY', required=True)
@click.pass_context
@_exit_on_error
def evict(ctx: click.Context, key: str) -> None:
    """Evict object from an endpoint."""
    with _endpoint_client(ctx) as client:
        client.evict(key, ctx.obj['TARGET_ENDPOINT_ID'])
    _success('Evicted object from endpoint.')


@client_group.command()
@click.argument('key', metavar='KEY', required=True)
@click.pass_context
@_exit_on_error
def exists(ctx: click.Context, key: str) -> None:
    """Check if object exists in an endpoint."""
    with _endpoint_client(ctx) as client:
        res = client.exists(key, ctx.obj['TARGET_ENDPOINT_ID'])
    click.echo(f'Object exists: {res}')


@client_group.command()
@click.argument('key', metavar='KEY', required=True)
@click.pass_context
@_exit_on_error
def get(ctx: click.Context, key: str) -> None:
    """Get an object from an endpoint."""
    with _endpoint_client(ctx) as client:
        res = client.get(key, ctx.obj['TARGET_ENDPOINT_ID'])

    if res is None:
        _note('Object does not exist.')
    else:
        obj = deserialize(res)
        click.echo(f'Result: {obj}')


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
@_exit_on_error
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
            result = client.ping(target)
            client_ms = (time.perf_counter() - start) * 1000

            if result.peer_rtt_ms is None:
                times.append(client_ms)
                click.echo(
                    f'Reply from local endpoint: time={client_ms:.2f} ms'
                )
                continue

            times.append(result.peer_rtt_ms)
            if result.relayed is None:  # pragma: no cover
                path = 'unknown'
            else:
                kind = click.style(
                    'relayed via' if result.relayed else 'direct to',
                    fg='yellow' if result.relayed else 'green',
                )
                path = (
                    f'{kind} {result.remote_addr} '
                    f'(rtt {result.path_rtt_ms} ms)'
                )
            click.echo(
                f'Reply from {target}: time={result.peer_rtt_ms:.2f} ms '
                f'path={path}',
            )

    click.secho(
        f'{len(times)} ping(s): min/avg/max = {min(times):.2f}/'
        f'{sum(times) / len(times):.2f}/{max(times):.2f} ms',
        bold=True,
    )


@client_group.command()
@click.argument('data', required=True)
@click.pass_context
@_exit_on_error
def put(ctx: click.Context, data: str) -> None:
    """Put an object in an endpoint."""
    key = str(uuid.uuid4())
    with _endpoint_client(ctx) as client:
        client.set(key, serialize(data), ctx.obj['TARGET_ENDPOINT_ID'])
    _success(f'Put object in endpoint with key {key}')
