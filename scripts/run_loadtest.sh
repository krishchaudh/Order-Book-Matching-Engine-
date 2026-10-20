#!/usr/bin/env bash
# gRPC load matrix against a server started by this script on localhost.
# Usage: scripts/run_loadtest.sh [OUT_DIR] [DURATION_S]
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=${1:-results}; DUR=${2:-10}
mkdir -p "$OUT"; rm -f "$OUT/grpc.jsonl"
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DME_BUILD_GRPC=ON >/dev/null && cmake --build build >/dev/null
SYMS=AAPL,MSFT,GOOG,AMZN
# Server: 2 shard threads pinned to cpus 2,3; client threads get the rest.
taskset -c 2-7 ./build/src/grpc/me_server --shards=2 --affinity=2,3 --symbols=$SYMS >"$OUT/server.log" 2>&1 &
SRV=$!; trap 'kill -TERM $SRV 2>/dev/null; wait $SRV 2>/dev/null' EXIT
sleep 1
L="taskset -c 0,1,4-7 ./build/src/grpc/me_loadtest --target=localhost:50051 --symbols=$SYMS --duration=$DUR --warmup=2 --metrics=localhost:9100 --json=$OUT/grpc.jsonl"
$L --mode=unary  --concurrency=1,2,4,8,16,32,64 --channels=8 --label=localhost-unary
$L --mode=stream --concurrency=1,2,4,8 --inflight=1  --channels=8 --label=localhost-stream-inflight1
$L --mode=stream --concurrency=1,2,4,8 --inflight=32 --channels=8 --label=localhost-stream-inflight32
echo "results in $OUT/grpc.jsonl"
