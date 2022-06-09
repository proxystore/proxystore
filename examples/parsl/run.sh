#!/bin/bash

PORT=59465
redis-server --save "" --appendonly no --port $PORT &> /dev/null &
REDIS=$!

ARRAY_COUNT=100
ARRAY_SIZE=100

echo "Started Redis on localhost:$PORT"

echo "Running mapreduce_parsl.py without ProxyStore"
python mapreduce_parsl.py -n $ARRAY_SIZE -s $ARRAY_SIZE

echo "Running mapreduce_parsl.py with ProxyStore (Local)"
python mapreduce_parsl.py -n $ARRAY_SIZE -s $ARRAY_SIZE --proxy

echo "Running mapreduce_parsl.py with ProxyStore (Redis)"
python mapreduce_parsl.py -n $ARRAY_SIZE -s $ARRAY_SIZE --proxy --redis-port $PORT

kill $REDIS
