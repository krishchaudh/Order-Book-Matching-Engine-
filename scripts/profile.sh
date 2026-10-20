#!/usr/bin/env bash
# perf stat + perf record (+ flamegraph if inferno/FlameGraph is installed) on
# one engine workload. Usage: scripts/profile.sh [workload] [book]
set -euo pipefail
cd "$(dirname "$0")/.."
W=${1:-mixed}; BOOK=${2:-dense}; OUT=results/perf; mkdir -p "$OUT"
cmake -S . -B build-prof -G Ninja -DCMAKE_BUILD_TYPE=Release -DME_NATIVE=ON -DME_FRAME_POINTERS=ON -DME_BUILD_GRPC=OFF -DME_BUILD_TESTS=OFF >/dev/null
cmake --build build-prof --target me_bench >/dev/null
B="./build-prof/me_bench --ops=5000000 --trials=3 --pin=2 --quiet --workload=$W --book=$BOOK"
perf stat -e task-clock,cycles,instructions,branches,branch-misses,cache-references,cache-misses,L1-dcache-loads,L1-dcache-load-misses,LLC-loads,LLC-load-misses,dTLB-load-misses \
  -o "$OUT/stat-$W-$BOOK.txt" $B
cat "$OUT/stat-$W-$BOOK.txt"
perf record -F 2000 -g --call-graph fp -o "$OUT/perf-$W-$BOOK.data" $B >/dev/null
perf report -i "$OUT/perf-$W-$BOOK.data" --stdio --no-children --percent-limit 1 2>/dev/null | grep -v '^$' | head -60 > "$OUT/report-$W-$BOOK.txt"
cat "$OUT/report-$W-$BOOK.txt"
if command -v inferno-collapse-perf >/dev/null; then
  perf script -i "$OUT/perf-$W-$BOOK.data" | inferno-collapse-perf | inferno-flamegraph > "docs/flamegraph-$W.svg"
elif [ -x "${FLAMEGRAPH_DIR:-/opt/FlameGraph}/flamegraph.pl" ]; then
  perf script -i "$OUT/perf-$W-$BOOK.data" | "${FLAMEGRAPH_DIR:-/opt/FlameGraph}/stackcollapse-perf.pl" | "${FLAMEGRAPH_DIR:-/opt/FlameGraph}/flamegraph.pl" > "docs/flamegraph-$W.svg"
fi
