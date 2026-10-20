# Low-latency order matching engine (C++17)

[![CI](https://github.com/krishchaudh/Order-Book-Matching-Engine-/actions/workflows/ci.yml/badge.svg)](https://github.com/krishchaudh/Order-Book-Matching-Engine-/actions/workflows/ci.yml)

An exchange-style central limit order book with price-time priority, a sharded
single-writer execution engine, a gRPC/Protobuf API, Prometheus metrics, a Go
health sidecar, and Docker / Envoy / Kubernetes deployment — built to be
measured. Every number below was produced by code in this repo with the
commands in [docs/PERFORMANCE.md](docs/PERFORMANCE.md); raw output is committed
under [`results/`](results/).

| | Measured (8-vCPU AWS VM, GCC 11, `-O3 -march=native`) |
|---|---|
| Single-thread matching throughput, realistic mixed flow | **19.1 M ops/s** (target was 5 M) |
| Single-thread, worst workload (every op is a full fill) | **7.5 M ops/s** |
| Aggregate, 8 independent symbol shards | **77.7 M ops/s** |
| Matching latency, mixed flow | p50 **113 ns**, p99 **507 ns**, p99.9 740 ns |
| Heap allocations per operation | **0** |
| gRPC unary, localhost, 8 clients | **61.9 K req/s**, p50 110 µs, **p99 432 µs** |
| gRPC streaming, 4 streams × 32 in flight | **151 K req/s**, p99 3.3 ms |
| Tests | 143 (unit + randomized differential vs reference book), clean under ASan, UBSan, TSan |

In-memory matching throughput and gRPC request throughput are different
measurements and are never mixed; see
[Benchmark methodology](#benchmark-methodology).

---

## Contents

1. [Architecture](#architecture)
2. [Matching algorithm and semantics](#matching-algorithm-and-semantics)
3. [Order-book data structures](#order-book-data-structures)
4. [Concurrency design](#concurrency-design)
5. [Performance optimizations](#performance-optimizations)
6. [Benchmark methodology](#benchmark-methodology)
7. [Measured results](#measured-results)
8. [gRPC API](#grpc-api)
9. [Observability](#observability)
10. [Deployment: Docker, Envoy, Kubernetes](#deployment-docker-envoy-kubernetes)
11. [Testing](#testing)
12. [Build and run](#build-and-run)
13. [Trade-offs and limitations](#trade-offs-and-limitations)
14. [Repository layout](#repository-layout)

---

## Architecture

```
                         one process = one partition of the symbol space
            ┌──────────────────────────────────────────────────────────────────┐
 clients ──▶│ gRPC handler threads            MPSC ring + futex     shard threads│
 Envoy  ──▶ │  SubmitOrder / CancelOrder  ───────────────────────▶ ┌───────────┐│
 (x-me-     │  GetOrderBook / GetBestBidAsk ─────────────────────▶ │ shard 0   ││
 partition) │  SubmitOrderStream (bidi)   ◀── completion (seq,    │ OrderBook ││
            │                                 status, fills)      │  per sym  ││
            │                                                     ├───────────┤│
            │ metrics thread  /metrics /readyz /livez  :9100      │ shard 1 … ││
            └──────────────────────────────────────────────────────┴───────────┘│
                   ▲ scrape                       ▲ probe (stall detection)
             Prometheus / Grafana           Go health sidecar  :8081
```

* **Book** (`include/me/order_book.h`): one symbol, single-threaded, pure. All
  state is in flat arrays; the hot path never allocates.
* **Engine** (`include/me/engine.h`): symbol table, validation, per-book
  sequence numbers, one `OrderBook` per symbol.
* **Sharded engine** (`include/me/sharded_engine.h`): *N* shard threads, each
  exclusively owning a disjoint set of symbols; lock-free MPSC request queues
  in, completions out.
* **gRPC server** (`src/grpc/server.cpp`): converts protobuf ⇄ engine types,
  routes to the owning shard, waits for the completion, fills the execution
  report. Rejects symbols this partition does not own (`FAILED_PRECONDITION`).
* **Metrics** (`src/metrics/`): per-shard counters written by the owning
  thread, histograms, Prometheus text exposition, readiness/liveness.
* **Sidecar** (`sidecar/`): Go service probing `/metrics` and the gRPC port;
  flips readiness when the engine stops making progress while requests arrive.

Full description: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Matching algorithm and semantics

* **Price-time priority.** Each price level is a FIFO of orders. An incoming
  buy sweeps ask levels from the lowest upward while `ask ≤ limit`; a sell
  sweeps bid levels from the highest downward while `bid ≥ limit`. Within a
  level, oldest order first.
* **Trade price is the resting (maker) order's price.** The aggressor's limit
  is only a bound. Example: book has ask 101×10 (first) and 102×10; a buy limit
  105×15 trades 10 @ 101 then 5 @ 102, and nothing rests.
* **Partial fills** reduce the maker's `remaining` in place; it keeps its
  queue position. A limit remainder rests; a **market remainder is canceled**
  (market orders never rest; a market order on an empty book is canceled with
  zero fills).
* **Sequence numbers.** Every accepted submit, fill and cancel gets a
  per-book monotonically increasing `seq`. Output is a pure function of the
  input stream, so replaying the same inputs reproduces execution exactly
  (this is what the differential tests rely on).
* **Integers only.** `Price` is `int64_t` ticks, `Qty` is `uint64_t`. No
  floating point anywhere in the matching path.
* **Rejections are explicit:** `INVALID_ORDER_ID` (0), `DUPLICATE_ORDER_ID`
  (already open in that book), `INVALID_QUANTITY` (0), `INVALID_PRICE` /
  `PRICE_OUT_OF_RANGE` (limit ≤ 0 or ≥ `max_price`), `UNKNOWN_SYMBOL`,
  `UNKNOWN_ORDER` (cancel of a non-open id), `BOOK_FULL`, `INVALID_SIDE`,
  `INVALID_ORDER_TYPE`, `WRONG_PARTITION`.
  A rejected request changes no state and consumes no sequence number.
* **Not supported** (by design, see limitations): modify/replace,
  IOC/FOK/stop orders, self-trade prevention, fees.

## Order-book data structures

Production configuration: `OrderBook = OrderBookT<DenseLevelIndex, OpenAddressOrderIndex>`.
The book is a template over its two indexes so that alternatives can be
benchmarked with the *same* matching code (`--book=dense|dense_std|map|map_oa|sortedvec|reference`).

| Component | Implementation | Why |
|---|---|---|
| Order storage | `OrderPool`: flat array of 32-byte, 32-byte-aligned `Order` slots (two per cache line), intrusive free list, lazily committed `mmap` | zero allocations; slot index (`uint32_t`) instead of pointers halves link size |
| Price level FIFO | intrusive doubly-linked list through `Order::prev/next` slot indices; `Level{head, tail, count, qty}` | O(1) append, O(1) unlink once the slot is known |
| Price → level | `DenseLevelIndex`: `Level[max_price]` indexed directly by tick, plus a two-tier 64-ary bitmap of non-empty prices (`HierBitmap`) | best bid/ask = `bsr`/`tzcnt` over ≤ 3 words; next-level walk is a bit scan, not a tree step |
| Order id → slot | `OpenAddressOrderIndex`: flat open addressing, linear probing, backward-shift deletion (no tombstones), load factor ≤ ½, locality-preserving *grouped* hash | one cache line per lookup; no rehash debt from tombstones |
| Trades out | caller-owned `std::vector<Trade>` reused across calls | the fill path does not allocate after warm-up |

Alternatives kept for comparison: `MapLevelIndex` (`std::map<Price,Level>`),
`SortedVecLevelIndex`, `StdOrderIndex` (`std::unordered_map`), and
`ReferenceBook` (naive STL book used as the differential-test oracle).
Measured head-to-head in [docs/PERFORMANCE.md §5](docs/PERFORMANCE.md#5-baseline-vs-optimized).

## Concurrency design

* **Single writer per book.** A shard thread is the only thread that ever
  touches its books. No locks, no atomics, no false sharing in the matching
  path; determinism is preserved because each book sees one total order of
  requests.
* **Static symbol → shard mapping** inside a process (`fnv1a64(symbol) % shards`)
  and **symbol → partition** across processes (`fnv1a64(symbol) % partitions`).
  A process registers only the symbols it owns; a request for a symbol it does
  not own is rejected with `WRONG_PARTITION` rather than matched against a
  duplicate book. Exactly one active owner per symbol, by construction.
* **Hand-off** is a bounded MPSC ring per shard (`MPSCQueue`): producers CAS a
  ticket, write the slot, publish with a release store; the consumer spins
  briefly then parks on a futex. A synchronous round trip costs ~430 ns
  (p99 750 ns); pipelined, 7.7 M requests/s per shard.
* **Reads** (`GetOrderBook`, `GetBestBidAsk`) go through the same queue, so a
  snapshot is always consistent with the execution order.

## Performance optimizations

The three largest, each measured by swapping only that component
(5 M ops, pinned, median of trials — full tables in
[docs/PERFORMANCE.md §5](docs/PERFORMANCE.md#5-baseline-vs-optimized)):

| Optimization | Replaces | Effect |
|---|---|---|
| **Dense price array + hierarchical bitmap** for levels | `std::map<Price,Level>` | deep book **+120 %**, cancel +118 %, mixed +63 % |
| **Flat open-addressing order index**, backward-shift delete, 0 allocs | `std::unordered_map` | cancel **+160 %**, mixed +85 %, deep +64 %; 0.61 → 0 allocs/op |
| **Grouped locality hash** (4 consecutive ids share one cache line) | Fibonacci hash | sequential-id insert **+72 %**, neutral on mixed |

Combined, versus a `std::map` + `std::unordered_map` book with identical
matching logic: mixed **+134 %**, deep **+271 %**. Versus the STL reference
book: mixed 20×, cancel 146×.

Also: intrusive pool and FIFO (zero allocations on every workload), reusable
trade buffers, lazily committed `mmap` tables, MPSC pipelining (4× over
synchronous hand-off). Rejected after measurement: pre-sizing the index to
capacity (mixed −35 %), a pure fold hash (fragile), transparent huge pages (no
effect on this VM). Hardware perf counters are not available in this VM, so
the profile is sample-based; after removing allocation and tree walks the
remaining cost is the random cache line of the order-id index.

## Benchmark methodology

`benchmarks/bench_main.cpp` (`me_bench`) drives the real `OrderBook`:

* Release build, `-O3 -march=native`, thread pinned with `--pin=CPU`.
* Deterministic seed; every operation is generated **before** the timed loop,
  and replayed against a shadow book during generation so the expected trade
  count, trade checksum, open-order count and filled volume are known.
* Warm-up trial, then *K* timed trials; **median** reported with min/max.
* After every trial the engine's results are compared with the expected
  values and `check_invariants()` runs; a mismatch prints `INVALID` and the
  trial is discarded — nothing can be optimized away unnoticed.
* No no-op operations. Every counted op is a submit or cancel against a
  stateful book; cancels target live orders, aggressors target the touch.
* Latency is measured in a separate `--latency` run (per-op timestamps cost
  throughput), and single-thread vs. multi-shard aggregate are always labelled.
* Allocations per op are counted by a global `operator new` hook; peak RSS
  and CPU time come from `getrusage`.
* Environment facts (CPU, kernel, compiler, flags, governor, THP) are printed
  into the results. Frequency scaling could **not** be controlled on this VM.

Workloads: `insert`, `cancel`, `match`, `partial`, `mixed`
(60/30/10 insert/cancel/aggress, configurable with `--mix=`), `deep`
(5 000-tick spread, 2 M resting), `multisym` (64 symbols), `shards`
(independent pinned engines), `queue_sync`/`queue_pipelined`. Exact definitions
in [docs/PERFORMANCE.md §3](docs/PERFORMANCE.md#3-workloads).

## Measured results

One thread pinned to CPU 2, 5 000 000 ops × 5 trials, production book
(`results/engine.txt`):

| Workload | M ops/s (median) | p50 | p99 | p99.9 | allocs/op |
|---|---:|---:|---:|---:|---:|
| insert | 18.89 | 45 ns | 322 ns | 441 ns | 0 |
| cancel | 9.51 | 291 ns | 582 ns | 745 ns | 0 |
| match (every op a full fill) | 7.54 | 180 ns | 476 ns | 632 ns | 0 |
| partial (every op a partial fill) | 25.68 | 60 ns | 129 ns | 182 ns | 0 |
| mixed | **19.08** | **113 ns** | **507 ns** | 740 ns | 0 |
| deep | 11.89 | 115 ns | 533 ns | 737 ns | 0 |
| multisym (64) | 14.26 | — | — | — | 0 |

Independent shards (aggregate, `mixed`): 2 threads 34.3 M, 4 threads
62.6 M, 8 threads 77.7 M ops/s (the 8 vCPUs are 4 physical cores with SMT).

gRPC, localhost, client-observed (`results/grpc.jsonl`, server 2 shards
pinned, 8 HTTP/2 channels, 10 s per row):

| Mode | Clients | req/s | p50 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| unary | 1 | 20 606 | 35.6 µs | 230 µs | 2.0 ms |
| unary | 8 | **61 881** | 110 µs | **432 µs** | 2.7 ms |
| unary | 16 | 63 655 | 202 µs | 2.1 ms | 3.7 ms |
| stream, 1 in flight | 4 | 62 936 | 42 µs | 239 µs | 2.4 ms |
| stream, 32 in flight | 4 | **151 496** | 330 µs | 3.3 ms | 9.5 ms |

Separate Docker containers on a bridge network (unpinned): unary 8 clients
50.3 K req/s, p99 1.6 ms; streaming 4×32 152 K req/s; streaming 4×1 46.2 K
req/s, p99 961 µs. Kubernetes networking was **not**
measured (no cluster available). Everything else, including distributions,
hardware, flags, profiles and the baseline comparison, is in
[docs/PERFORMANCE.md](docs/PERFORMANCE.md).

## gRPC API

`proto/matching_engine.proto`, package `me.v1`:

| RPC | Purpose |
|---|---|
| `SubmitOrder(SubmitOrderRequest) → ExecutionReport` | limit/market order; report carries status, seq, fills (trades with maker/taker ids, price, qty), remaining qty, reject reason |
| `CancelOrder(CancelOrderRequest) → CancelOrderResponse` | cancel an open order; returns status, seq, canceled qty or reject reason |
| `GetOrderBook(GetOrderBookRequest) → GetOrderBookResponse` | top-N levels per side with aggregate qty and order count |
| `GetBestBidAsk(GetBestBidAskRequest) → GetBestBidAskResponse` | touch prices and sizes |
| `ListSymbols` | symbols owned by this partition |
| `SubmitOrderStream(stream SubmitOrderRequest) → stream ExecutionReport` | bidirectional; many requests in flight per stream |

Errors: every rejection the engine can express (`INVALID_ORDER_ID`,
`DUPLICATE_ORDER_ID`, `INVALID_QUANTITY`, `INVALID_PRICE`, `PRICE_OUT_OF_RANGE`,
`UNKNOWN_SYMBOL`, `UNKNOWN_ORDER`, `BOOK_FULL`, `INVALID_SIDE`,
`INVALID_ORDER_TYPE`, `WRONG_PARTITION`) comes back as a normal response with
`status=REJECTED` and a `reject_reason` enum, so a client always gets a typed
answer. gRPC status codes are used only where there is no response to return:
`FAILED_PRECONDITION` (symbol owned by another partition, with the owner in the
message), `NOT_FOUND` (unknown symbol on a query), `UNAVAILABLE` (shutting
down).

Clients: `me_client` (CLI: `submit`, `cancel`, `book`, `bba`, `symbols`,
`stream`, and `demo`, which walks through FIFO, maker-price execution, market
sweep, duplicate id, invalid qty and unknown symbol) and `me_loadtest`
(unary/stream, concurrency ramp, warm-up, deterministic mix, p50–p99.9,
error rate, server CPU/RSS scraped from `/metrics`, JSON output).

## Observability

Prometheus text format on `:9100/metrics`: `me_orders_received_total`,
`me_cancels_received_total`, `me_orders_accepted_total`,
`me_orders_rejected_total`, `me_orders_canceled_total`,
`me_trades_executed_total`, `me_volume_traded_total`, `me_operations_total`,
`me_matching_latency_ns` and `me_queue_latency_ns` (histograms, shard-side),
`me_grpc_requests_total`, `me_grpc_errors_total`, `me_grpc_latency_ns`
(histogram, handler-side), `me_active_orders`, `me_symbols`,
`me_book_depth_levels` / `me_book_best_price` per symbol and side, process CPU
and RSS. Counters are plain stores by the owning shard thread; nothing in
`OrderBook` itself is instrumented. `/readyz` reports whether the shards are
running and accepting; `/healthz` whether the metrics thread responds.

`docker/prometheus.yml` scrapes engines, sidecars and Envoy;
`docker/grafana/dashboard.json` is auto-provisioned by Compose (ops/s,
latency histograms, rejects, depth, CPU/RSS, sidecar health). The dashboard
JSON was written against the exported metric names but has not been visually
reviewed in a browser.

## Deployment: Docker, Envoy, Kubernetes

* `docker/Dockerfile.engine` — multi-stage build that runs the unit tests
  during the image build and ships `me_server`, `me_client`, `me_loadtest`,
  `me_bench` in a non-root Ubuntu image with a `/readyz` `HEALTHCHECK`.
* `docker/Dockerfile.sidecar` — Go 1.22 static binary in a distroless image.
* `docker/docker-compose.yml` — two engine partitions (`--partition=0/2`,
  `1/2`), a sidecar per engine, Envoy, Prometheus, Grafana.
* `envoy/envoy.yaml` — HTTP/2 gRPC ingress, active gRPC health checks,
  outlier ejection, circuit breakers, admin/stats on `:9901`. Routes by the
  `x-me-partition` header to the owning partition; without the header it
  round-robins, and a mis-routed request fails fast with `WRONG_PARTITION`
  (a load balancer must not spread one symbol over replicas).
* `kubernetes/` — `StatefulSet` of engine partitions (pod ordinal = partition
  index, headless service for stable names, sidecar container, readiness /
  liveness probes, requests/limits, `terminationGracePeriodSeconds` with
  SIGTERM-driven graceful drain, `PodDisruptionBudget`), Envoy `Deployment`
  + `HorizontalPodAutoscaler`, Prometheus, and a load-generator `Job`.
  Scaling partitions means changing `replicas` **and** `ME_PARTITION_COUNT`
  together — this remaps symbols and does not migrate open orders.

Validation status: images build and run; engine ⇄ load tester across a Docker
bridge network measured; sidecar endpoints verified against a live engine;
`docker compose config` and `kubectl kustomize` render cleanly. **Not
exercised:** Envoy end-to-end, any real Kubernetes cluster.

## Testing

```
./build/me_tests            # 143 tests
```

* Unit: FIFO priority, partial/full fills, crossed orders, multiple levels,
  cancel first/middle/last, invalid orders, duplicate ids, empty book, market
  orders (incl. empty book and partial liquidity), multiple symbols, depth
  snapshots, best bid/ask, sequence numbers, high-volume sequences.
* Differential: randomized operation streams run against `OrderBook` and the
  naive `ReferenceBook`; trades, statuses, sequence numbers, depth and
  invariants must match exactly. Both the dense and the alternative indexes
  are covered.
* Data structures: `OpenAddressOrderIndex`, `DenseLevelIndex`/`HierBitmap`,
  `OrderPool` edge cases (wrap-around, delete-under-probe, growth).
* Concurrency: `ShardedEngine` with many producer threads, restart, shutdown
  with in-flight requests; `MPSCQueue` stress.
* Metrics exporter and sidecar (`go test ./...`).
* Sanitizers: ASan+UBSan and TSan builds of the full suite pass
  (`-DME_SANITIZE=address,undefined` / `thread`; TSan needs
  `setarch x86_64 -R` on kernels with large ASLR entropy).
* CI (`.github/workflows/ci.yml`): release build + tests, benchmark smoke run,
  gRPC server/client/load-test smoke, sanitizer matrix, Go vet/test, Docker
  image builds, Compose config validation.

## Build and run

```bash
# Ubuntu 22.04 toolchain
sudo apt-get install -y build-essential cmake ninja-build libgtest-dev libbenchmark-dev \
    libprotobuf-dev protobuf-compiler libgrpc++-dev protobuf-compiler-grpc

cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DME_BUILD_GRPC=ON
cmake --build build
./build/me_tests

# server + demo client
./build/src/grpc/me_server --shards=2 --symbols=AAPL,MSFT,GOOG,AMZN &
./build/src/grpc/me_client demo
./build/src/grpc/me_client book AAPL 5
curl -s localhost:9100/metrics | head

# benchmarks (native build, pinned)
scripts/run_benchmarks.sh results 5000000 5          # full matrix → results/engine.txt
./build-bench/me_bench --workload=mixed --ops=5000000 --trials=5 --pin=2 --latency

# gRPC load matrix → results/grpc.jsonl
scripts/run_loadtest.sh results 10

# profile (perf record + report; flamegraph if inferno/FlameGraph is installed)
scripts/profile.sh mixed

# containers
docker compose -f docker/docker-compose.yml up --build    # Envoy :8080, Prometheus :9090, Grafana :3000
./build/src/grpc/me_client --target=localhost:50051 bba AAPL

# kubernetes
kubectl apply -k kubernetes/
kubectl -n matching-engine apply -f kubernetes/loadgen-job.yaml
```

CMake options: `ME_BUILD_TESTS`, `ME_BUILD_BENCHMARKS`, `ME_BUILD_GRPC`,
`ME_NATIVE` (`-march=native`), `ME_FRAME_POINTERS`, `ME_SANITIZE=<list>`,
and `-DCMAKE_CXX_FLAGS=-DME_OA_HASH={0,1,2}` to select the order-index hash.

## Trade-offs and limitations

* **No durability or replication.** Books are in memory; a restart loses
  resting orders. Kubernetes restarts the *same* partition so ownership is
  never duplicated, but clients must re-enter orders. Production fault
  tolerance is **not** claimed.
* **No partition migration.** Changing the partition count remaps symbols
  without moving open orders.
* **Dense price array** costs `sizeof(Level) × max_price` of (lazily
  committed) address space per book and requires prices to fit in
  `[1, max_price)`; the sparse `MapLevelIndex` is available for instruments
  with huge tick ranges.
* **Flat hash index** pays for rehashes when a single book grows to millions
  of resting orders; `std::unordered_map` wins the artificial 5 M-insert
  workload (31 vs 19 M/s) and loses everywhere else.
* **Synchronous gRPC server**: ~62 K unary req/s with sub-ms p99 on this VM;
  the async completion-queue API would be the next step.
* Tails: p99.99 of microseconds and max of milliseconds from page faults and
  VM preemption; no pre-faulting or core isolation.
* Measurement environment: 8-vCPU SMT VM, hidden CPU model, no frequency
  control, no hardware perf counters.
* Unverified here: Envoy end-to-end, Kubernetes in a real cluster, the Grafana
  dashboard visually, isolated metrics overhead.

## Repository layout

```
include/me/        engine headers (order_book.h, level_index.h, order_index.h, order_pool.h,
                   engine.h, sharded_engine.h, reference_book.h, metrics.h, memory.h, types.h)
src/grpc/          server.cpp, client.cpp, loadtest.cpp
src/metrics/       Prometheus exporter + HTTP endpoint
proto/             matching_engine.proto
benchmarks/        bench_main.cpp (me_bench), microbench.cpp (Google Benchmark)
tests/             GoogleTest suites incl. differential fuzzing
sidecar/           Go health sidecar + tests
docker/            Dockerfiles, docker-compose.yml, prometheus.yml, grafana/
envoy/             envoy.yaml
kubernetes/        StatefulSet, Envoy, Prometheus, load-gen Job, kustomization
scripts/           run_benchmarks.sh, run_loadtest.sh, profile.sh
docs/              ARCHITECTURE.md, PERFORMANCE.md
results/           committed raw measurements
```
