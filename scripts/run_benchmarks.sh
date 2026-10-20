#!/usr/bin/env bash
# Reproduces every number in docs/PERFORMANCE.md (engine side).
# Usage: scripts/run_benchmarks.sh [OUT_DIR] [OPS] [TRIALS]
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=${1:-results}; OPS=${2:-5000000}; TRIALS=${3:-5}
mkdir -p "$OUT"
cmake -S . -B build-bench -G Ninja -DCMAKE_BUILD_TYPE=Release -DME_NATIVE=ON -DME_BUILD_GRPC=OFF -DME_BUILD_TESTS=OFF >/dev/null
cmake --build build-bench --target me_bench >/dev/null
B=./build-bench/me_bench
{
  echo "# $(date -u +%FT%TZ)  $(uname -srm)"; grep -m1 'model name' /proc/cpuinfo; nproc
  echo "# governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unavailable)"
  echo "# THP: $(cat /sys/kernel/mm/transparent_hugepage/enabled)"
  echo "# gcc: $(gcc --version | head -1)"
} | tee "$OUT/env.txt"
rm -f "$OUT/engine.jsonl"; cp /dev/null "$OUT/engine.txt"
# 1. Throughput, all workloads, single pinned thread (no per-op clock reads).
$B --ops="$OPS" --trials="$TRIALS" --pin=2 --json="$OUT/engine.jsonl" --workload=insert
for w in cancel match partial mixed deep multisym; do $B --ops="$OPS" --trials="$TRIALS" --pin=2 --json="$OUT/engine.jsonl" --workload=$w --quiet; done
# 2. Per-operation latency distributions (adds ~20 ns/op of clock overhead).
for w in insert cancel match partial mixed deep; do $B --ops="$OPS" --trials="$TRIALS" --pin=2 --latency --json="$OUT/engine.jsonl" --workload=$w --quiet; done
# 3. Aggregate throughput across independent shards.
for t in 1 2 4 6 8; do $B --ops="$OPS" --trials="$TRIALS" --pin=0 --threads=$t --json="$OUT/engine.jsonl" --workload=shards --quiet; done
# 4. Queue hand-off cost (what a gRPC handler pays on top of the book).
$B --ops=1000000 --trials="$TRIALS" --pin=2 --latency --json="$OUT/engine.jsonl" --workload=queue_sync --quiet
$B --ops=1000000 --trials="$TRIALS" --pin=2 --json="$OUT/engine.jsonl" --workload=queue_pipelined --quiet
# 5. Data-structure alternatives on the same mixed workload (baseline comparison).
for b in map dense_std map_oa sortedvec; do
  for w in insert cancel mixed deep; do $B --ops="$OPS" --trials=3 --pin=2 --book=$b --json="$OUT/engine.jsonl" --workload=$w --quiet; done
done
# The STL reference book cancels in O(n); it is a correctness oracle, so it is
# only run at 200k ops to show the gap (minutes per trial at 5M).
for w in insert cancel mixed deep; do $B --ops=200000 --prefill=20000 --trials=3 --pin=2 --book=reference --json="$OUT/engine.jsonl" --workload=$w --quiet; done
# 6. Id-locality sensitivity: scrambled (random-looking) ids vs sequential.
for w in insert cancel mixed deep; do $B --ops="$OPS" --trials=3 --pin=2 --ids=scrambled --json="$OUT/engine.jsonl" --workload=$w --quiet; done
echo "results in $OUT/engine.jsonl"
