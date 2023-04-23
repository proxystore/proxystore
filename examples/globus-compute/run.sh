#!/bin/bash

PORT=59465
redis-server --save "" --appendonly no --port $PORT &> redis.out &
REDIS=$!

ARRAY_COUNT=10
ARRAY_SIZE=10

echo "Started Redis on localhost:$PORT"

echo "Running mapreduce_globus_compute.py without ProxyStore"
python mapreduce_globus_compute.py -e $ENDPOINT -n $ARRAY_SIZE -s $ARRAY_SIZE

echo "Running mapreduce_globus_compute.py with ProxyStore (File)"
python mapreduce_globus_compute.py -e $ENDPOINT -n $ARRAY_SIZE -s $ARRAY_SIZE \
    --ps-file --ps-file-dir /tmp/proxystore-dump

echo "Running mapreduce_globus_compute.py with ProxyStore (Redis)"
python mapreduce_globus_compute.py -e $ENDPOINT -n $ARRAY_SIZE -s $ARRAY_SIZE \
    --ps-redis --ps-redis-port $PORT

echo "Stopping Redis"
kill $REDIS
