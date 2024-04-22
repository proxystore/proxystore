#!/bin/bash

PORT=59465
redis-server --save "" --appendonly no --port $PORT &> /dev/null &
REDIS=$!

ARRAY_COUNT=10
ARRAY_SIZE=1000

echo "Started Redis on localhost:$PORT"

echo "Running mapreduce_parsl.py without ProxyStore"
python mapreduce_parsl.py -n $ARRAY_COUNT -s $ARRAY_SIZE

echo "Running mapreduce_parsl.py with ProxyStore (Local)"
python mapreduce_parsl.py -n $ARRAY_COUNT -s $ARRAY_SIZE --proxy

echo "Running mapreduce_parsl.py with ProxyStore (Redis)"
python mapreduce_parsl.py -n $ARRAY_COUNT -s $ARRAY_SIZE --proxy --redis-port $PORT

kill $REDIS
