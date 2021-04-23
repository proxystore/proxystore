#!/bin/bash

PORT=59465
redis-server --port $PORT &> redis.out &
REDIS=$!

ARRAY_COUNT=100
ARRAY_SIZE=100

echo "Started Redis on localhost:$PORT"

echo "Running mapreduce.py without ProxyStore"
python mapreduce.py -n $ARRAY_SIZE -s $ARRAY_SIZE

echo "Running mapreduce.py with ProxyStore (Local)"
python mapreduce.py -n $ARRAY_SIZE -s $ARRAY_SIZE --proxy

echo "Running mapreduce.py with ProxyStore (Redis)"
python mapreduce.py -n $ARRAY_SIZE -s $ARRAY_SIZE --proxy --redis-port $PORT

kill $REDIS
