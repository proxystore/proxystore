r"""Peer endpoint transfer speed test.

Start the remote endpoint in one process (or on one system):

```bash
$ python -m testing.scripts.peer_endpoint_bandwidth remote --relays none
Endpoint ID: 4c1d...
Direct addresses: 127.0.0.1:52341, ...
```

Then start the local endpoint in another process (or on another system)
with the ID and optionally an address of the remote:

```bash
$ python -m testing.scripts.peer_endpoint_bandwidth local 4c1d... \
    --relays none --addr 127.0.0.1:52341
```

Without `--addr`, the remote is found using n0's DNS discovery which requires
the default `--relays n0`.

Warning:
    The iroh bindings copy each byte of data passed to Rust in a Python loop
    which limits throughput. The `--memmove-patch` option replaces the copy
    with `ctypes.memmove()`. This patches generated code in the bindings so
    it is only used for benchmarking until the bindings include the fix.
    Use the option for both the local and remote endpoints because the data
    of a GET response is written by the remote.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import ctypes
import logging
import os
import statistics
import sys
import tempfile
import time
from collections.abc import Sequence
from typing import Any

import iroh

from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.identity import generate_secret_key
from proxystore.endpoint.peers import Allowlist
from proxystore.p2p.manager import PeerManager


class _AllowAll(Allowlist):
    """Allowlist which allows all peers for benchmarking."""

    def reload(self) -> set[EndpointId]:
        return set()

    def allowed(self, endpoint_id: EndpointId) -> bool:
        return True

    def name_of(self, endpoint_id: EndpointId) -> str | None:
        return 'peer'


def apply_memmove_patch() -> None:
    """Replace the per-byte copy of bytes arguments in the iroh bindings."""
    from iroh import iroh_ffi

    def _fast_write(self: Any, value: bytes) -> None:
        n = len(value)
        with self._reserve(n):
            dst = ctypes.addressof(self.rbuf.data.contents) + self.rbuf.len
            ctypes.memmove(dst, bytes(value), n)

    iroh_ffi._UniffiRustBufferBuilder.write = _fast_write  # type: ignore[method-assign]


def _relay_options(relays: str) -> dict[str, Any]:
    if relays == 'n0':
        return {'preset': iroh.preset_n0()}
    return {
        'preset': iroh.preset_n0(),
        'relay_mode': iroh.RelayMode.disabled(),
        'online_timeout': None,
    }


async def _endpoint(relays: str, tmp_dir: str) -> Endpoint:
    manager = PeerManager(
        generate_secret_key(),
        _AllowAll(os.path.join(tmp_dir, 'peers.toml')),
        **_relay_options(relays),
    )
    return await Endpoint('benchmark', manager.id, peer_manager=manager)


async def _time(coro: Any) -> float:
    start = time.perf_counter()
    await coro
    return time.perf_counter() - start


async def run_local(
    remote: EndpointId,
    addrs: list[str],
    sizes: list[int],
    repeat: int,
    relays: str,
    tmp_dir: str,
) -> None:
    """Measure transfer speeds to the remote endpoint."""
    endpoint = await _endpoint(relays, tmp_dir)
    assert endpoint.peer_manager is not None
    if len(addrs) > 0:
        endpoint.peer_manager.add_peer_addr(
            iroh.EndpointAddr(
                iroh.EndpointId.from_string(remote), None, addrs
            ),
        )

    try:
        connect = await _time(endpoint.exists('key', remote))
        print(f'Connection established in {connect * 1000:.1f} ms')

        rtts = [
            await _time(endpoint.exists('key', remote)) for _ in range(100)
        ]
        print(f'Round trip (EXISTS): {statistics.median(rtts) * 1000:.3f} ms')

        print(f'{"Size (B)":>12} {"SET (Mbps)":>12} {"GET (Mbps)":>12}')
        for size in sizes:
            data = os.urandom(size)
            set_times = [
                await _time(endpoint.set('key', data, remote))
                for _ in range(repeat)
            ]
            get_times = [
                await _time(endpoint.get('key', remote)) for _ in range(repeat)
            ]
            await endpoint.evict('key', remote)
            set_mbps = size * 8 / 1e6 / min(set_times)
            get_mbps = size * 8 / 1e6 / min(get_times)
            print(f'{size:>12} {set_mbps:>12.1f} {get_mbps:>12.1f}')
    finally:
        await endpoint.close()


async def run_remote(relays: str, tmp_dir: str) -> None:
    """Serve an endpoint until interrupted."""
    endpoint = await _endpoint(relays, tmp_dir)
    assert endpoint.peer_manager is not None
    addr = endpoint.peer_manager.addr()
    print(f'Endpoint ID: {endpoint.id}')
    print(f'Direct addresses: {", ".join(addr.direct_addresses())}')
    print('Serving remote endpoint. Use ctrl-C to stop')
    try:
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.Event().wait()
    finally:
        await endpoint.close()


def main(argv: Sequence[str] | None = None) -> int:
    """Peer endpoint bandwidth app."""
    argv = argv if argv is not None else sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Measure transfer speed between two endpoints.',
    )
    parser.add_argument(
        'actor',
        choices=['local', 'remote'],
        help='should this process act as the local or remote endpoint',
    )
    parser.add_argument(
        'remote_id',
        nargs='?',
        help='ID of the remote endpoint (required for local)',
    )
    parser.add_argument(
        '--addr',
        action='append',
        default=[],
        help='direct address of the remote endpoint (can be repeated)',
    )
    parser.add_argument(
        '--sizes',
        type=int,
        nargs='+',
        default=[1_000, 1_000_000, 10_000_000],
        help='sizes in bytes of data to transfer',
    )
    parser.add_argument(
        '--repeat',
        type=int,
        default=3,
        help='repetitions of each transfer (the best is reported)',
    )
    parser.add_argument(
        '--relays',
        choices=['n0', 'none'],
        default='n0',
        help='use n0 relays or disable relays',
    )
    parser.add_argument(
        '--memmove-patch',
        action='store_true',
        help='patch the iroh bindings to copy bytes with ctypes.memmove()',
    )
    parser.add_argument(
        '--no-uvloop',
        action='store_true',
        help='override using uvloop if available',
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='enable debug logging',
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.WARNING)

    if args.memmove_patch:
        apply_memmove_patch()
        print('Applied memmove patch to iroh bindings')

    run: Any = asyncio.run
    if not args.no_uvloop:
        try:
            import uvloop

            run = uvloop.run
        except ImportError:  # pragma: no cover
            print('uvloop unavailable... using default asyncio event loop')

    with tempfile.TemporaryDirectory() as tmp_dir:
        if args.actor == 'local':
            if args.remote_id is None:
                parser.error('the remote_id is required for local')
            coro = run_local(
                EndpointId.from_str(args.remote_id),
                args.addr,
                args.sizes,
                args.repeat,
                args.relays,
                tmp_dir,
            )
        else:
            coro = run_remote(args.relays, tmp_dir)
        with contextlib.suppress(KeyboardInterrupt):
            run(coro)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
