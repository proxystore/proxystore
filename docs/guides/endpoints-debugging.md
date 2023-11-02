# Endpoints Debugging

*Last updated 2 May 2023*

This guide outlines some common trouble-shooting steps to take if you
are encountering issues using ProxyStore Endpoints.

## Test a Local Endpoint

Consider you configured and started an endpoint as follows:
```bash
$ proxystore-endpoint configure myendpoint
INFO: Configured endpoint myendpoint <f4dc841d-377e-4785-8d66-8eade34f63cd>. Start with:
INFO:   $ proxystore-endpoint start myendpoint
$ proxystore-endpoint start myendpoint
INFO: Starting endpoint process as daemon.
INFO: Logs will be written to ~/.local/share/proxystore/myendpoint/log.txt
```

### Check Endpoint Logs
Endpoint logs are written to a directory in `$XDG_DATA_HOME/proxystore` which
in this case is `~/.local/share/proxystore/myendpoint`
(see [`home_dir()`][proxystore.utils.environment.home_dir] for the full
specification).
```bash
$ tail -n 1 ~/.local/share/proxystore/myendpoint/log.txt
INFO  (uvicorn.error) :: Uvicorn running on http://127.0.1.1:8766 (Press CTRL+C to quit)
```
The logs are the first place to check for any potential issues.

### Monitor the Endpoint
Debug level logging can be enabled when starting the endpoint, and
the endpoint can be run directly in the terminal instead of as a daemon process
via the `--no-detach` flag. These two options are helpful for live monitoring
the endpoint.
```bash
$ proxystore-endpoint --log-level DEBUG start myendpoint --no-detach
```

### Use the Test CLI
The `proxystore-endpoint` CLI provides a `test` subcommand for testing endpoint commands.
See the [CLI Reference](../api/cli.md#proxystore-endpoint-test){target=_blank}.
```bash
$ proxystore-endpoint test myendpoint exists abcdef
INFO: Object exists: False
```
As expected, an object with key `abcdef` does not exist in the store, but
we got a valid response so we know the endpoint is running correctly.
You can also validate that this request was logged by the endpoint.

### Invoke a REST Request
Endpoints serve a REST API so `curl` can be used to check if an endpoint
is accessible. Note the `proxystore-endpoint test` CLI is preferred for
debugging. The correct address the endpoint is listening on can be found in
the logs.
```bash
$ curl http://127.0.0.1:8765/exists?key=abcdef
{"exists": false}
```

## Test a Remote Endpoint

Consider I have an endpoint running on system A with UUID
`aaaa0259-5a8c-454b-b17d-61f010d874d4` and another on System B
with UUID `bbbbab4d-c73a-44ee-a316-58ec8857e83a`.

### Check Relay Server Connections
Both endpoints must be connected to the same relay server to form a peer
connection. First, check the `address` value in the `[relay]` section
is present and set to the correct URI string.
The endpoint config is found in the `config.toml` file in the endpoint
directory (e.g., `~/.local/share/proxystore/myendpoint/config.toml`).
Restart your endpoints if you had to change the configuration.

Second, confirm the endpoint connects to the relay server when started by
checking the endpoint logs for a line like this.
```bash
INFO  (proxystore.p2p.relay_client) :: Established client connection to relay server at ws://localhost:8765 with client uuid=aaaa0259-5a8c-454b-b17d-61f010d874d4 and name=myendpoint
```

### Use the Test CLI
The `proxystore-endpoint test` CLI can be used to establish a peer connection
between two endpoints and invoke remote operations.
Here, we will request the endpoint on system A (named "myendpoint") to invoke
an `exists` operation on the endpoint on system B.
```bash
$ proxystore-endpoint test --remote bbbbab4d-c73a-44ee-a316-58ec8857e83a myendpoint exists abcdef
INFO: Object exists: False
```

You will get an error if the peer connection fails. For example:
```bash
ERROR: Endpoint returned HTTP error code 500. Request to peer bbbbab4d-c73a-44ee-a316-58ec8857e83a failed: ...
```
If this happens, check the logs for both endpoints for further error messages.
Peer requests typically fail for two reasons:

1. One of the endpoints is not running (e.g., an endpoint crashed) or is not
   connected to the relay server.
2. One of the endpoints is behind a symmetric NAT. The NAT traversal
   techniques used to establish peer-to-peer connections between endpoints
   are not reliable across symmetric NATs or poorly behaved legacy NATs.

### Check Peer-to-Peer Compatibility
After ensuring both endpoints are running and connected to the relay server,
you can check the NAT compatibility in two ways.

1. Endpoints will attempt to discover and log the NAT type on startup, so check
   the logs to see if this could be the reason.
   ```
   INFO  (proxystore.p2p.nat) :: Checking NAT type. This may take a moment...
   INFO  (proxystore.p2p.nat) :: NAT Type:       Full-cone NAT
   INFO  (proxystore.p2p.nat) :: External IP:    <IP ADDRESS>
   INFO  (proxystore.p2p.nat) :: External Port:  <PORT>
   INFO  (proxystore.p2p.nat) :: NAT traversal for peer-to-peer methods (e.g., hole-punching) is likely to work. (NAT traversal does not work reliably across symmetric NATs or poorly behaved legacy NATs.)
   ```
2. Use the
   [`proxystore-endpoint check-nat`](../api/cli.md#proxystore-endpoint-check-nat)
   command to discover your NAT type.
   ```
   $ proxystore-endpoint check-nat
   INFO: Checking NAT type. This may take a moment...
   INFO: NAT Type:       Full-cone NAT
   INFO: External IP:    <IP ADDRESS>
   INFO: External Port:  <PORT>
   INFO: NAT traversal for peer-to-peer methods (e.g., hole-punching) is likely to work. (NAT traversal does not work reliably across symmetric NATs or poorly behaved legacy NATs.)
   ```
