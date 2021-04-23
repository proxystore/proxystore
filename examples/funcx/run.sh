#!/bin/bash

PORT=59465
redis-server --port $PORT &> redis.out &
REDIS=$!

ARRAY_COUNT=10
ARRAY_SIZE=10

echo "Started Redis on localhost:$PORT"

echo "Starting local default FuncX endpoint"
funcx-endpoint start default

echo "Running mapreduce.py without ProxyStore"
time python mapreduce.py -n $ARRAY_SIZE -s $ARRAY_SIZE -f $FUNCX_ENDPOINT

echo "Running mapreduce.py with ProxyStore (Redis)"
time python mapreduce.py -n $ARRAY_SIZE -s $ARRAY_SIZE -f $FUNCX_ENDPOINT --proxy --redis-port $PORT

kill $REDIS
funcx-endpoint stop default
funcx-endpoint stop default
