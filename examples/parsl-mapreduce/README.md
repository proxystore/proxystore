# MapReduce with Parsl and ProxyStore

Example of integrating ProxyStore into a Parsl app.

```
$ python mapreduce_parsl.py -n 10 -s 1000 --proxy --redis-port $PORT
$ bash run.sh
```

Based on https://parsl.readthedocs.io/en/stable/userguide/workflow.html#mapreduce
