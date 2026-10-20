# Performance report

All numbers in this document were produced by the code in this repository on the
machine described below, by the exact commands listed in
[Reproduction](#reproduction). Nothing is extrapolated. Raw output is committed in
`results/` (`engine.txt`, `engine.jsonl`, `grpc.jsonl`, `hash_matrix.txt`, `perf/`).

> **Two different things are measured here.** *Matching throughput* is the
> single-threaded in-memory order book driven directly by a benchmark harness.
> *gRPC throughput/latency* is what a client observes over a TCP/HTTP-2 connection.
> They differ by roughly two orders of magnitude and are never combined.

## 1. Summary

| Metric | Target | Actual (median of trials) |
|---|---|---|
| Single-core matching throughput, realistic mixed workload | 5 M ops/s | **19.08 M ops/s** (one thread, pinned) |
| Single-core matching throughput, worst workload (`match`: every op is a full fill) | 5 M ops/s | **7.54 M ops/s** |
| Aggregate matching throughput, 8 independent shards (8 vCPU VM) | maximize | **77.7 M ops/s** (4 shards: 62.6 M) |
| Matching p50 latency (mixed) | minimize | **113 ns** |
| Matching p99 latency (mixed) | < 1 ms | **507 ns** (p99.9 740 ns, max 3.2 ms) |
| gRPC p99 latency, unary, localhost, 8 concurrent clients | < 1 ms | **432 µs** at 61.9 K req/s |
| gRPC p99 latency, unary, localhost, 1 client | < 1 ms | **230 µs** (p50 35.6 µs) |
| gRPC sustainable throughput with p99 < 1 ms | maximize | **61.9 K req/s** unary (c=8); **62.9 K req/s** streaming (c=4, 1 in flight) |
| gRPC peak throughput (p99 > 1 ms) | — | **151 K req/s** streaming, 4 streams × 32 in flight, p99 3.3 ms (152 K across Docker bridge) |
| Peak RSS, benchmark process, mixed workload (5.1 M orders generated, ~100 K resting) | minimize | 370 MB (includes the harness's pre-generated op stream and shadow book) |
| Peak RSS, 5 M resting orders (`insert`) | — | 1 187 MB (same caveat) |
| Heap allocations per operation (production engine) | 0 | **0.0000** on every workload |

**The 5 M ops/s target is met by a single matching thread on every workload,
including the trade-heavy ones, with p99 under a microsecond.** Multi-shard
numbers are *aggregate* throughput of independent books and are reported
separately. gRPC request rates are 100–300× lower than in-memory matching rates;
the gRPC stack, not the matching engine, is the bottleneck on that path.

## 2. Environment

| | |
|---|---|
| Machine | AWS VM, 8 vCPU "Intel(R) Xeon(R) Processor" (SMT siblings; the exact model is hidden by the hypervisor), ~31 GiB RAM, no swap |
| Kernel | Linux 6.8.0-1061-aws x86_64, Ubuntu 22.04.5 |
| Compiler | GCC 11.4.0, `-std=c++17 -O3 -DNDEBUG -march=native` (CMake `Release` + `-DME_NATIVE=ON`) |
| CPU affinity | Benchmarks pinned with `--pin=2` (`sched_setaffinity`); shard runs pin thread *i* to CPU *i*. gRPC server shards pinned to CPUs 2,3, load-test client on CPUs 0,1,4–7. |
| Frequency scaling | **Not controllable**: `/sys/devices/system/cpu/cpu*/cpufreq` is absent in this VM. Turbo/steal effects are visible as trial-to-trial variance; min/max of every trial set is printed next to the median. |
| Transparent huge pages | `madvise` mode; the engine `madvise(MADV_HUGEPAGE)`s its large tables but `AnonHugePages` stayed at 0 kB, so no THP benefit is claimed. |
| Hardware perf counters | **Unavailable** (`cycles`, `instructions`, `branch-misses`, `cache-misses`, … all `<not supported>` under `perf stat`). Only `task-clock`, page faults and sampled call stacks (`perf record -g`, software clock) were available. Cache/branch analysis below is therefore based on sampling profiles and A/B experiments, not on counter values. |
| Sanitizers | 143/143 tests pass under ASan+UBSan and under TSan (TSan needs `setarch x86_64 -R` on this kernel). |

## 3. Workloads

All workloads are generated **before** the timed region by a seeded generator
(`--seed=42`) and replayed against a *shadow* book during generation so the
harness knows the expected trade count, trade checksum, open-order count and
filled volume. After each trial the real engine's results are compared against
these and `book.check_invariants()` is run; a mismatch prints `INVALID` and the
trial is discarded. There are no no-op operations: every counted op is a real
submit or cancel against a stateful book.

Default parameters: 1 symbol, mid price 10 000, spread ±50 ticks, prefill
100 000 resting orders (prefill is applied before the timer starts), 5 000 000
timed operations per trial, 5 trials, median reported.

| Workload | Definition |
|---|---|
| `insert` | 5 M non-crossing limit orders spread over 100 price levels (book grows to 5.1 M resting orders) |
| `cancel` | Prefill 5 M resting orders, then cancel all of them in uniformly random order |
| `match` | Prefill 5 M resting orders (qty 10); each timed op is an aggressive limit at the touch that fully fills exactly one resting order (5 M trades) |
| `partial` | Prefill 50 K large resting orders (qty 1000); every op is a small aggressor (qty 7) partially filling the maker |
| `mixed` | Steady state around a 100 K-order book: 60 % passive insert / 30 % cancel of a random live order / 10 % aggressive (20 % of those are market orders). Distribution is configurable with `--mix=`. |
| `deep` | `mixed` with spread 5 000 ticks and 2 M resting orders (deep, wide book) |
| `multisym` | `mixed` over 64 symbols in one engine (symbol chosen uniformly per op) |
| `shards` | *T* independent engines on *T* pinned threads, each running `mixed`; reports aggregate ops/s |
| `queue_sync` / `queue_pipelined` | Producer thread → MPSC queue → shard thread → completion, i.e. the `ShardedEngine` path the gRPC server uses; synchronous = 1 request in flight, pipelined = batched hand-off |

## 4. Results — matching engine (production configuration)

`OrderBookT<DenseLevelIndex, OpenAddressOrderIndex>`, one thread pinned to CPU 2,
5 M ops × 5 trials. `results/engine.txt` has the unabridged output.

### 4.1 Throughput

| Workload | Median M ops/s | min – max | Trades | Peak RSS |
|---|---:|---:|---:|---:|
| insert | **18.89** | 18.14 – 23.65 | 0 | 1 187 MB |
| cancel | **9.51** | 9.01 – 9.70 | 0 | 1 344 MB |
| match | **7.54** | 7.32 – 7.69 | 5 000 000 | 1 336 MB |
| partial | **25.68** | 24.19 – 25.83 | 5 005 380 | 337 MB |
| mixed | **19.08** | 18.33 – 19.78 | 1 081 653 | 370 MB |
| deep | **11.89** | 10.48 – 12.68 | 743 431 | 793 MB |
| multisym (64) | **14.26** | 13.79 – 15.44 | 1 052 560 | 906 MB |

`allocs/op` was 0.0000 for every row (global `operator new` is counted by the
harness). CPU utilisation was 100 % of one core in every row. Peak RSS is for the
whole benchmark process; it includes the pre-generated 5 M-op stream and the
shadow book, so it is an upper bound on engine memory.

### 4.2 Latency distribution (per-operation, same workloads)

`--latency` timestamps every operation with `clock_gettime(CLOCK_MONOTONIC)`
before and after; the ~25 ns clock calls and the loss of the tight replay loop
reduce throughput, which is why these runs are reported separately and the
throughput column of this table must **not** be compared with 4.1.

| Workload | mean | p50 | p90 | p95 | p99 | p99.9 | p99.99 | max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| insert | 74 ns | 45 | 172 | 183 | **322** | 441 | 496 | 111 µs |
| cancel | 313 ns | 291 | 426 | 491 | **582** | 745 | 9 796 | 2.4 ms |
| match | 212 ns | 180 | 333 | 388 | **476** | 632 | 4 930 | 2.3 ms |
| partial | 69 ns | 60 | 92 | 104 | **129** | 182 | 4 139 | 2.0 ms |
| mixed | 143 ns | 113 | 288 | 355 | **507** | 740 | 5 316 | 3.2 ms |
| deep | 169 ns | 115 | 327 | 384 | **533** | 737 | 6 176 | 3.6 ms |

The p99.99 and max tails (microseconds to milliseconds) are page faults on
first touch of lazily committed pool/index pages and VM preemption; they are
real, and a production deployment would pre-fault (`MAP_POPULATE`) and isolate
cores. Everything up to p99.9 is sub-microsecond.

### 4.3 Aggregate throughput — independent shards

Each shard is a separate engine on its own pinned core running `mixed`; there is
no sharing between shards. This is **not** single-book throughput.

| Threads | Aggregate M ops/s (median) | min – max | Scaling vs 1 thread |
|---:|---:|---:|---:|
| 1 | 11.35 ¹ | 9.58 – 17.63 | — |
| 2 | 34.35 | 31.05 – 35.30 | — |
| 4 | **62.61** | 58.29 – 63.83 | 3.3× of the 19.08 single-thread figure |
| 6 | 42.17 | 28.21 – 54.98 | very noisy (SMT siblings + host contention) |
| 8 | **77.72** | 60.70 – 94.43 | 4.1× |

¹ The 1-thread `shards` row is pinned to CPU 0, which also serves interrupts and
the host agent on this VM; its spread (9.6–17.6) shows why the single-core numbers
are taken on CPU 2. Beyond 4 threads the 8 vCPUs are SMT siblings sharing 4
physical cores, so scaling flattens and variance grows.

### 4.4 Queue hand-off (the path the gRPC server uses)

| Mode | M ops/s | Round-trip latency p50 / p99 / p99.9 |
|---|---:|---|
| `queue_sync` (1 in flight: enqueue → shard → completion) | 1.91 | 430 ns / 750 ns / 1.39 µs |
| `queue_pipelined` (batched, many in flight) | 7.69 | — |

A synchronous cross-thread round trip costs ~430 ns on this VM, i.e. about 4×
the matching work itself. Pipelining removes most of it; the gRPC server does the
same thing implicitly because many handler threads submit concurrently.

## 5. Baseline vs optimized

### 5.1 Reference implementation (the differential-test oracle)

`ReferenceBook` is a deliberately simple STL book (`std::map` of price levels,
linear scans for cancel). It is correct, it is what the fuzzer compares against,
and it is the honest "first version". 200 K ops, 20 K prefill, 3 trials:

| Workload | Reference | Production | Speed-up |
|---|---:|---:|---:|
| insert | 15.67 M/s | 18.89 M/s | 1.2× |
| cancel | 0.065 M/s | 9.51 M/s | **146×** |
| mixed | 0.945 M/s | 19.08 M/s | **20×** |
| deep | 0.317 M/s | 11.89 M/s | **38×** |

### 5.2 Data-structure A/B (same harness, same workloads, 5 M ops, 3 trials)

The engine is a template over the level index and the order index, so each
alternative is the *same* matching code with one component swapped.

| Configuration (`--book=`) | Level index | Order-id index | insert | cancel | mixed | deep | allocs/op (mixed) |
|---|---|---|---:|---:|---:|---:|---:|
| `dense` **(production)** | dense array + 2-level bitmap | open addressing | 18.89 | **9.51** | **19.08** | **11.89** | 0 |
| `dense_std` | dense array + bitmap | `std::unordered_map` | **31.02** | 3.66 | 10.34 | 7.26 | 0.61 |
| `map_oa` | `std::map<Price,Level>` | open addressing | 13.21 | 4.37 | 11.74 | 5.40 | 0.03 |
| `map` | `std::map<Price,Level>` | `std::unordered_map` | 18.17 | 3.29 | 8.17 | 3.20 | 0.64 |
| `sortedvec` | sorted `std::vector` of levels | open addressing | 15.93 | 5.34 | 11.59 | 6.15 | 0 |

Read-outs:

* **Dense levels + bitmap vs `std::map` levels** (`dense` vs `map_oa`, same order
  index): cancel +118 %, mixed +63 %, deep **+120 %**. Best-price discovery is a
  `bsr`/`tzcnt` over three 64-bit words instead of a pointer-chasing tree walk,
  and a level is one array slot instead of a red-black node.
* **Open-addressing order index vs `std::unordered_map`** (`dense` vs
  `dense_std`, same levels): cancel **+160 %**, mixed +85 %, deep +64 %, and
  allocations drop from 0.61/op to 0. The one place `unordered_map` wins is pure
  5 M-order insertion (31.0 vs 18.9 M/s): the flat table doubles and rehashes 12
  times on the way to 2²³ slots, while `unordered_map`'s per-node allocations are
  cheap individually. Pre-sizing the table to `max_open_orders` removes the
  rehashes (insert 15.0 → 21.8 M/s in that experiment) but made `mixed` 35 %
  *slower* because a 2²³-slot cold table defeats the cache; it was rejected.
  Realistic books hold far fewer resting orders than the 5 M in `insert`, so the
  mixed/deep/cancel columns are the ones that matter.
* Both together (`dense` vs `map`): mixed **+134 %**, deep **+271 %**, cancel +189 %.

### 5.3 Hash function for the order index (`results/hash_matrix.txt`)

Client order ids are usually near-sequential. A Fibonacci-mixed hash scatters
consecutive ids across the table (one cache miss per lookup); a plain fold keeps
them adjacent but clusters badly under linear probing; the shipped **grouped**
hash keeps runs of 4 consecutive ids in one 64-byte line and scatters the groups.
5 M ops, pinned, 3 trials, M ops/s:

| Workload | ids | Fibonacci | fold | **grouped (default)** |
|---|---|---:|---:|---:|
| insert | sequential | 12.68 | 52.01 | **21.84** (+72 %) |
| cancel | sequential | 9.79 | 7.86 | 9.03 (−8 %) |
| mixed | sequential | 19.73 | 15.51 | 19.63 (±0) |
| deep | sequential | 11.67 | 11.14 | 12.31 (+5 %) |
| insert | scrambled | 12.74 | 14.44 | 13.48 |
| cancel | scrambled | 10.06 | 11.35 | 9.41 |
| mixed | scrambled | 19.99 | 17.47 | 16.34 (noisy: 12.6–18.3) |
| deep | scrambled | 10.37 | 10.25 | 8.85 |

`fold` is fastest on pure sequential insertion but degrades under deletion
(backward-shift on long runs) and is fragile under adversarial ids, so grouped
was chosen as the balanced default. `--ids=scrambled` applies a bijective
multiplicative permutation to the ids so the worst case can always be measured;
the production engine stays well above 5 M ops/s there too (13.0 / 9.56 / 18.17 /
11.45 M ops/s for insert / cancel / mixed / deep in the final run).

### 5.4 Other optimizations in the engine (kept)

* **Order pool + intrusive FIFO.** Orders live in a flat array of 32-byte,
  32-byte-aligned slots (two per cache line); `prev`/`next` are 32-bit slot
  indices, not pointers. Cancel is O(1) unlink after the index lookup. Result:
  zero heap allocations per operation in every workload.
* **Trades are written into a caller-owned `std::vector<Trade>` that is reused**
  across operations (the gRPC server keeps one per handler thread), so even the
  fill path does not allocate after warm-up.
* **Lazy, `mmap`-backed large tables** (`include/me/memory.h`) with
  `MAP_NORESERVE`: a book configured for 4 M orders costs no RSS until orders
  arrive. `MADV_HUGEPAGE` is requested; no measurable gain on this VM (THP never
  materialised), kept because it is free.
* **MPSC queue with futex parking**: pipelined cross-thread hand-off at 7.7 M
  ops/s, 4× the synchronous round trip.

### 5.5 Rejected experiments

| Experiment | Result | Decision |
|---|---|---|
| Pre-size order index to `max_open_orders` | insert +46 %, **mixed −35 %** | rejected (cache-cold table) |
| Transparent huge pages via `madvise` | no change (`AnonHugePages` 0 kB on this VM) | kept (harmless), no claim |
| `fold` hash | insert +310 %, cancel −20 %, fragile under adversarial ids | rejected as default; selectable with `-DME_OA_HASH=1` |

## 6. Profile (what the single thread spends its time on)

`scripts/profile.sh mixed` / `insert` (`perf record -g`, software clock since
hardware events are unsupported here; `results/perf/report-*.txt`).

`mixed`, 5 M ops: 2.53 s task-clock for 3 trials + generation.

| Share | Symbol | Note |
|---:|---|---|
| 18.2 % | `OpenAddressOrderIndex::erase` | cancel + fully-filled makers; backward-shift deletion touches the probe run |
| 15.9 % | `Generator::mixed_stream` | **harness**, outside the timed region |
| 12.1 % | `Runner::apply_all` (inlined `OrderBook::submit`/`cancel`/matching loop) | the matching logic itself |
| 9.1 % | `OpenAddressOrderIndex::find` | first touch of the id → slot line for cancels |

`insert`, 5 M ops: `Runner::trial` (inlined book insert + level/bitmap update)
46.5 %, `OpenAddressOrderIndex::find` 18.3 % (duplicate-id check), kernel page
faults 6.2 %, `OpenAddressOrderIndex::insert` 5.1 %.

Conclusion: after removing allocation and tree walks, the remaining cost is
**memory latency on the order-id index** (one random cache line per lookup — the
book itself is cache-resident). That is also why the grouped hash helps and why
`cancel` is the slowest single-op workload. Further gains would come from
prefetching the index line for the *next* request while matching the current one
(possible in the gRPC server where requests are queued, not in a synchronous
API), or from client-side slot hints.

## 7. Results — gRPC (client-observed)

Setup: `scripts/run_loadtest.sh results 10`. `me_server --shards=2
--affinity=2,3`, 4 symbols, client `me_loadtest` on CPUs 0,1,4–7 with 8 HTTP/2
channels, mix 60 % submit / 25 % cancel / 15 % aggressive, 2 s warm-up, 10 s
measurement per row, localhost TCP (loopback). `srv_cpu%` is the server process
CPU (all threads) and `srv_rss` its RSS, both scraped from the server's own
`/metrics`. Error rate was 0.000 % in every row (every request got a valid
engine response; business rejects are counted separately in `rej%`, also 0).

### 7.1 Unary `SubmitOrder` / `CancelOrder`

| Concurrent clients | req/s | p50 µs | p90 µs | p99 µs | p99.9 µs | max µs | server CPU | RSS |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 20 606 | 35.6 | 50.2 | **230** | 2 026 | 7 613 | 153 % | 37 MB |
| 2 | 34 472 | 45.5 | 66.6 | 202 | 2 160 | 9 885 | 222 % | 45 MB |
| 4 | 56 680 | 57.2 | 79.2 | 212 | 2 292 | 10 139 | 252 % | 61 MB |
| 8 | **61 881** | 110.3 | 151.0 | **432** | 2 727 | 10 672 | 241 % | 86 MB |
| 16 | 63 655 | 202.3 | 319.0 | 2 132 | 3 727 | 10 639 | 276 % | 97 MB |
| 32 | 59 742 | 422.7 | 791.5 | 3 021 | 5 925 | 17 280 | 332 % | 140 MB |
| 64 | 53 620 | 1 021.9 | 2 346.8 | 4 645 | 8 213 | 19 765 | 360 % | 149 MB |

Unary throughput saturates at ~62 K req/s around 8–16 clients; p99 stays below
1 ms up to 8 clients (432 µs) and crosses it at 16. The p99.9 ≈ 2 ms in *every*
row, even at 1 client, is the gRPC completion-queue / thread wake-up path and VM
scheduling, not the engine (whose internal round trip is 0.75 µs at p99).

### 7.2 Streaming `SubmitOrderStream` (bidirectional, one stream per client)

| Streams | In flight / stream | req/s | p50 µs | p99 µs | p99.9 µs | server CPU |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 19 690 | 35.1 | 213 | 2 265 | 129 % |
| 2 | 1 | 42 254 | 36.0 | 135 | 2 240 | 197 % |
| 4 | 1 | **62 936** | 42.2 | **239** | 2 430 | 256 % |
| 8 | 1 | 50 054 | 50.6 | 2 134 | 5 135 | 274 % |
| 1 | 32 | 45 063 | 151.9 | 2 542 | 4 648 | 180 % |
| 2 | 32 | 94 030 | 227.2 | 2 467 | 6 476 | 256 % |
| 4 | 32 | **151 496** | 330.2 | 3 280 | 9 474 | 308 % |
| 8 | 32 | 69 511 | 575.8 | 32 849 | 51 608 | 278 % |

Streaming with a 32-deep window reaches **151 K req/s** (2.4× unary) at the cost
of queueing latency (p99 3.3 ms). With one request in flight per stream it
behaves like unary with less per-call overhead (p99 239 µs at 63 K req/s). The
8-stream rows collapse because client + server then need more than the 8 vCPUs
available; that is a load-generator limit of this machine, not a server limit.

### 7.3 Separate containers (Docker bridge network)

Server and load tester in two containers on a user-defined bridge network
(`docker network create me-net`), same host, same image (final build), 10 s per
row, **no CPU pinning** inside the containers (`results/grpc-docker.txt`):

| Mode | Clients | req/s | p50 µs | p99 µs | p99.9 µs | server CPU |
|---|---:|---:|---:|---:|---:|---:|
| unary | 1 | 13 721 | 38.4 | 583 | 2 783 | 133 % |
| unary | 8 | 50 254 | 118.1 | 1 615 | 3 322 | 278 % |
| unary | 32 | 37 373 | 567.4 | 4 379 | 7 709 | 355 % |
| stream, 1 in flight | 4 | 46 181 | 48.9 | 961 | 3 184 | 242 % |
| stream, 32 in flight | 1 | 34 357 | 217.0 | 3 306 | 5 382 | 165 % |
| stream, 32 in flight | 4 | **152 069** | 357.3 | 3 839 | 13 361 | 301 % |

Peak streaming throughput is unchanged by the bridge (152 K vs 151 K req/s);
unary p50 is within ~10 % of loopback, but the tails widen (p99 1.6 ms at 8
clients vs 432 µs on loopback) because the server's shard and gRPC threads are
no longer pinned and the veth/NAT path adds softirq work on the same 8 vCPUs.
An earlier run of the previous build with the same setup measured 53.9 K req/s
at p99 625 µs for unary/8, so this tail is run-to-run noise on a shared VM as
much as a bridge cost; pin the container (`--cpuset-cpus`) for stable tails.

### 7.4 Kubernetes

`kubernetes/` was validated with `kubectl kustomize` (15 objects render) and the
Docker images start and pass their probes, but **no Kubernetes cluster was
available in this environment**, so there is no in-cluster or Envoy-fronted
measurement. `kubernetes/loadgen-job.yaml` is the Job to run for that; see
`README.md` for the commands.

### 7.5 Where the gRPC time goes

Client-observed p50 at 1 client is 35.6 µs; of this the engine (queue → match →
completion) is ~0.5 µs. The rest is protobuf (de)serialisation, HTTP/2 framing,
two loopback TCP traversals, and the gRPC synchronous-server thread hand-offs.
The server is CPU-bound at ~250–360 % (gRPC threads) when it saturates — the two
shard threads are mostly idle. Raising gRPC throughput further means the async
completion-queue API with batched polling, not a faster order book.

## 8. Instrumentation overhead

Per-shard counters are plain stores by the owning thread (no atomics). The
latency histograms are updated once per request in the shard thread
(`ShardedEngineConfig::measure_latency`); the clock reads for them cost on the
order of the 25 ns `clock_gettime` pairs that distinguish table 4.2 from 4.1.
The isolated overhead of the histogram itself was **not** separately benchmarked
in this report; it is on the server path only, never in the `OrderBook`.

## 9. Remaining limitations

* 8-vCPU SMT VM, hidden CPU model, no frequency control, no hardware counters:
  the absolute numbers will differ on bare metal, and cache/branch-miss analysis
  is inferred, not counted.
* `insert` of 5 M orders into a flat open-addressing table pays for 12 rehashes;
  `std::unordered_map` is faster for that one (unrealistic) workload.
* p99.99 / max tails of microseconds–milliseconds from page faults and VM
  preemption; no pre-faulting or core isolation is done.
* gRPC: synchronous server API; ~62 K unary req/s with sub-ms p99 on this VM.
  No Kubernetes / Envoy path was measured.
* Instrumentation overhead not isolated.
* No durability, replication or failover (see `docs/ARCHITECTURE.md`).

## 10. Reproduction

```bash
# toolchain (Ubuntu 22.04)
sudo apt-get install -y build-essential cmake ninja-build libgtest-dev libbenchmark-dev \
    libprotobuf-dev protobuf-compiler libgrpc++-dev protobuf-compiler-grpc linux-tools-generic

# correctness first (results are only valid if this passes)
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DME_BUILD_GRPC=ON
cmake --build build && ./build/me_tests

# sanitizers
cmake -S . -B build-asan -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DME_SANITIZE=address,undefined -DME_BUILD_GRPC=OFF
cmake --build build-asan && ./build-asan/me_tests
cmake -S . -B build-tsan -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DME_SANITIZE=thread -DME_BUILD_GRPC=OFF
cmake --build build-tsan && setarch x86_64 -R ./build-tsan/me_tests

# full engine matrix (sections 4 and 5; ~20 min): results/engine.txt, results/engine.jsonl
scripts/run_benchmarks.sh results 5000000 5

# individual rows
./build-bench/me_bench --workload=mixed --ops=5000000 --trials=5 --pin=2            # 4.1
./build-bench/me_bench --workload=mixed --ops=5000000 --trials=5 --pin=2 --latency  # 4.2
./build-bench/me_bench --workload=shards --threads=4 --ops=5000000 --trials=5 --pin=0  # 4.3
./build-bench/me_bench --workload=mixed --book=map --ops=5000000 --trials=3 --pin=2 # 5.2
./build-bench/me_bench --workload=insert --ids=scrambled --ops=5000000 --pin=2      # 5.3
# hash variants: configure with -DCMAKE_CXX_FLAGS=-DME_OA_HASH={0,1,2}

# profiles (section 6): results/perf/
scripts/profile.sh mixed && scripts/profile.sh insert

# gRPC matrix (section 7.1/7.2): results/grpc.jsonl
scripts/run_loadtest.sh results 10

# containers (section 7.3)
docker build -f docker/Dockerfile.engine -t matching-engine:local .
docker network create me-net
docker run -d --rm --name me-engine --network me-net matching-engine:local \
    --shards=2 --symbols=AAPL,MSFT,GOOG,AMZN            # image ENTRYPOINT is me_server
docker run --rm --network me-net --entrypoint me_loadtest matching-engine:local \
    --target=me-engine:50051 --symbols=AAPL,MSFT,GOOG,AMZN --mode=unary --concurrency=1,8,32 --duration=10
docker run --rm --network me-net --entrypoint me_loadtest matching-engine:local \
    --target=me-engine:50051 --symbols=AAPL,MSFT,GOOG,AMZN --mode=stream --inflight=32 --concurrency=1,4 --duration=10
```
