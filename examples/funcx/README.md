# MapReduce with FuncX and ProxyStore

Example of integrating ProxyStore into a FuncX app.

```
$ python mapreduce_funcx.py -e $ENDPOINT -n 10 -s 100 --ps-redis --ps-redis-port $PORT
$ ENDPOINT={endpoint} bash run.sh
```

The example app supports three ProxyStore backends. If no option is specified,
ProxyStore is not used.

- **File**: `--ps-file --ps-file-dir /tmp/proxystore-dump`
- **Globus**: `--ps-globus --ps-globus-config globus-endpoint-config.json`
- **Redis**: `--ps-redis --ps-redis-port 1234`

See the [API docs](https://proxystore.readthedocs.io/en/latest/source/api.html)
for more info on any of the specific backend types.

To learn more about using FuncX, checkout the
[Tutorial](https://funcx.readthedocs.io/en/latest/Tutorial.html).
