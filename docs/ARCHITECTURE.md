# Architecture

```
                 ┌──────────────────────────────────────────────────────────────┐
  gRPC clients   │  me_server (one process = one partition of the symbol space) │
  ─────────────▶ │                                                              │
  Envoy (opt.)   │  gRPC handler threads ──┐   MPSC queue   ┌─ shard 0 thread   │
  x-me-partition │   SubmitOrder           ├──────────────▶ │  OrderBook[sym…]  │
                 │   CancelOrder           │   futex wake   ├─ shard 1 thread   │
                 │   GetOrderBook / BBA    ├──────────────▶ │  OrderBook[sym…]  │
                 │   SubmitOrderStream     │                └─ …                │
                 │                         ◀── completion (seq, status, fills)  │
                 │  metrics thread: HTTP /metrics /readyz /livez (port 9100)    │
                 └──────────────────────────────────────────────────────────────┘
                           ▲ scrape                     ▲ probe
                     Prometheus / Grafana        Go health sidecar (:8081)
```

## Layers

| Layer | Code | Role |
|---|---|---|
| Book | `include/me/order_book.h`, `level_index.h`, `order_index.h`, `order_pool.h` | Single-symbol CLOB. Pure, single-threaded, allocation-free on the hot path. |
| Engine | `include/me/engine.h` | Symbol table → `OrderBook` per symbol, validation, sequence numbers. |
| Sharded engine | `include/me/sharded_engine.h` | N shard threads, each exclusively owning a disjoint set of symbols; MPSC request queues. |
| API | `src/grpc/server.cpp`, `proto/matching_engine.proto` | gRPC façade; converts protobuf ⇄ engine types, routes to shard queue, waits for completion. |
| Observability | `include/me/metrics.h`, `src/metrics/metrics.cpp` | Lock-free per-shard counters, histograms, Prometheus text exporter, readiness/liveness. |
| Sidecar | `sidecar/main.go` | Probes the engine's metrics endpoint + gRPC port; detects stalls (requests arriving but operations not advancing). |

## Matching semantics

* Prices are `int64_t` ticks, quantities `uint64_t`. No floating point anywhere in matching.
* **Price-time priority.** Each price level is a FIFO (intrusive doubly-linked list of orders). The aggressor sweeps from the best opposite level while `aggressor price crosses level price` (buy: `limit ≥ ask`, sell: `limit ≤ bid`; market: always).
* **Trade price = resting (maker) order price.** The aggressor's limit is only a bound.
* Partial fills reduce the resting order's `remaining` in place; the order keeps its queue position.
* A limit remainder rests; a market remainder is **canceled** (never rests). Status `FILLED` / `PARTIALLY_FILLED` / `CANCELED` tells the client which.
* Every accepted submit, fill and cancel gets a per-book monotonically increasing `seq`. The sequence is a pure function of the input order, so replaying the same input yields byte-identical executions (the benchmark harness and differential tests depend on this).
* Rejections are explicit, never silent: `INVALID_ORDER_ID` (0), `DUPLICATE_ORDER_ID` (id already open in that book), `INVALID_QUANTITY` (0), `INVALID_PRICE` / `PRICE_OUT_OF_RANGE` (limit ≤ 0 or ≥ `max_price`), `UNKNOWN_SYMBOL`, `UNKNOWN_ORDER` (cancel of a non-open id), `BOOK_FULL`, `WRONG_PARTITION`. Order ids are unique per book while the order is open; a filled/canceled id may be reused (documented tradeoff: an exchange-grade system would keep a tombstone set).

## Order-book data structures (production configuration)

`OrderBook = OrderBookT<DenseLevelIndex, OpenAddressOrderIndex>`

* **`OrderPool`** — one `std::vector<Order>` of 32-byte, 32-byte-aligned order slots (two per cache line), reserved up to `max_open_orders` (pages are committed lazily by the kernel so a 4M-order reservation is cheap). Orders refer to each other with 32-bit slot indexes, not pointers: half the size, and the pool never reallocates while orders are live. A free-list threads through dead slots.
* **`DenseLevelIndex`** — a flat array `Level[max_price]` indexed directly by price, plus a two-tier 64-ary bitmap (`HierBitmap`) of non-empty levels. Best bid/ask = `bsr`/`tzcnt` on ≤ 3 words; "next non-empty level" during a sweep is the same operation. O(1) insert/remove/best for any price distribution; cost is memory proportional to the price range (2^20 levels × 24 B = 24 MiB virtual per side, touched pages only).
* **`OpenAddressOrderIndex`** — order-id → slot map: flat open addressing, linear probing, Fibonacci hashing, backward-shift deletion (no tombstones), load factor ≤ 0.5. One cache line per lookup in the common case.
* **Intrusive FIFO** — `Order{prev,next}` indices inside the level; cancel is O(1) given the slot.

Alternatives are kept (and benchmarked, see `docs/PERFORMANCE.md`): `MapLevelIndex` (`std::map<Price, Level>`), `SortedVecLevelIndex`, `StdOrderIndex` (`std::unordered_map`), and a deliberately naive `ReferenceBook` used only for differential testing.

## Concurrency model

* **Single writer per book.** A shard thread is the only thread that ever touches its books; there are no locks or atomics in the matching path.
* **Symbol → shard** is static: `shard = fnv1a64(symbol) % shards` inside a process; across processes `partition = fnv1a64(symbol) % partitions`. A process only *registers* symbols whose partition it owns, so a mis-routed request is rejected with `WRONG_PARTITION` instead of silently creating a second copy of the book.
* **Hand-off** is a bounded MPSC ring per shard (`MPSCQueue`): producers (gRPC handler threads) CAS a ticket, write the slot, publish with a sequence store; the consumer spins briefly then parks on a futex. The producer waits for its `Completion` by spin-then-futex. This costs ~1 µs round trip (`queue_sync` benchmark) — the dominant in-process overhead compared to the ~100 ns book operation, which is why the streaming RPC with many in-flight requests is the throughput path.
* **Reads** (`GetOrderBook`, `GetBestBidAsk`) are also serialized through the owning shard queue, so snapshots are consistent with execution order (simpler and correct; a seqlock snapshot would be the next optimization if read load mattered).

## Deployment topology

* `kubernetes/engine-statefulset.yaml`: one pod per partition; the pod ordinal is the partition index. Headless service gives each partition a stable DNS name. Readiness = engine `/readyz` (shards running) **and** the sidecar's stall detector.
* `envoy/envoy.yaml`: gRPC HTTP/2 front door with active gRPC health checks and outlier ejection; routes by `x-me-partition` header or round-robins (mis-routed requests fail fast with `FAILED_PRECONDITION`).
* Scaling: Envoy scales horizontally (HPA). Engine partitions scale by changing `replicas` **and** `ME_PARTITION_COUNT` together, which changes the symbol→partition mapping; this is a planned operation, not autoscaling.

## Limitations (deliberate, documented)

* **No durability.** Books live in memory only. A pod restart loses all resting orders; Kubernetes restarts the *same* partition (StatefulSet identity) so symbol ownership is never duplicated, but clients must re-enter orders. There is no journal, snapshot or replication. Nothing here claims fault tolerance beyond "the process restarts and is reachable again".
* **No partition migration.** Changing the partition count does not move open orders.
* **Market orders on an empty book** are canceled with zero fills (no "market-to-limit" conversion).
* **No self-trade prevention, no order modify/replace, no stop/IOC/FOK types, no fees.** Modify is expressible as cancel + new (loses queue position, as on most venues).
* **Order ids are client-supplied** and only required to be unique among *open* orders of a book.
