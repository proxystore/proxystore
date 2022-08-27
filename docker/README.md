# Docker Deployment for ProxyStore Components

*Note:* all commands are intended to be executed from the root of the repository.

## Signaling Server

### Build

```
docker build -t proxystore/signaling-server:latest -t proxystore/signaling-server:{VERSION} -f docker/Dockerfile-server .
```
Replace `{VERSION}` with the current ProxyStore version.

### Run

```
docker run --rm -it -p 8765:8765 --name signaling-server proxystore/signaling-server:latest
```

## Endpoint

Coming in the future.
