// ShardedEngine: N independent Engine instances, each owned by exactly one
// thread (single-writer), fed by an MPSC queue. Symbols are assigned to a
// shard by a stable hash of the symbol name, so every symbol has exactly one
// matching owner and execution on that symbol is deterministic.
//
// Callers (e.g. gRPC handlers) fill in a Request, enqueue it, and wait on its
// Completion (spin briefly, then futex sleep). Shard threads spin on their
// queue and park on a futex after a configurable idle period.
#pragma once

#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "me/engine.h"
#include "me/mpsc_queue.h"

namespace me {

inline uint64_t fnv1a64(const std::string& s) {
  uint64_t h = 14695981039346656037ull;
  for (unsigned char c : s) { h ^= c; h *= 1099511628211ull; }
  return h;
}

// Partition/shard assignment shared with the deployment layer (clients and
// Envoy routing use the same function to pick the owning engine instance).
inline uint32_t partition_of(const std::string& symbol, uint32_t partitions) {
  return static_cast<uint32_t>(fnv1a64(symbol) % partitions);
}

inline long futex_wait(std::atomic<uint32_t>* addr, uint32_t expected) {
  return syscall(SYS_futex, reinterpret_cast<uint32_t*>(addr), FUTEX_WAIT_PRIVATE, expected, nullptr, nullptr, 0);
}
inline long futex_wake(std::atomic<uint32_t>* addr, int n) {
  return syscall(SYS_futex, reinterpret_cast<uint32_t*>(addr), FUTEX_WAKE_PRIVATE, n, nullptr, nullptr, 0);
}

class Completion {
 public:
  void reset() { state_.store(0, std::memory_order_relaxed); }

  void signal() {
    if (state_.exchange(1, std::memory_order_acq_rel) == 2) futex_wake(&state_, 1);
  }

  void wait(uint32_t spin_iters = 20000) {
    for (uint32_t i = 0; i < spin_iters; ++i) {
      if (state_.load(std::memory_order_acquire) == 1) return;
      __builtin_ia32_pause();
    }
    uint32_t expected = 0;
    if (state_.compare_exchange_strong(expected, 2, std::memory_order_acq_rel)) {
      while (state_.load(std::memory_order_acquire) != 1) futex_wait(&state_, 2);
    }
  }

  bool done() const { return state_.load(std::memory_order_acquire) == 1; }

 private:
  std::atomic<uint32_t> state_{0};
};

enum class RequestType : uint8_t { Submit, Cancel, Snapshot, BestBidAsk, Shutdown };

struct Request {
  RequestType type = RequestType::Submit;
  SymbolId symbol = 0;  // shard-local id
  NewOrder order;
  OrderId cancel_id = 0;
  size_t depth = 10;
  // outputs
  SubmitResult submit_result;
  CancelResult cancel_result;
  DepthSnapshot snapshot;
  BestBidAsk bba;
  std::vector<Trade> trades;  // retains capacity if the Request is reused
  // Timing (steady-clock ns), filled when ShardedEngineConfig::measure_latency.
  uint64_t enqueue_ts_ns = 0;
  uint64_t queue_ns = 0;   // enqueue -> shard pickup
  uint64_t engine_ns = 0;  // time inside the order book
  uint64_t done_ts_ns = 0;
  Completion completion;
  // Optional hook: invoked on the shard thread after a submit/cancel
  // completes (used for metrics and execution-report fan-out).
  std::function<void(const Request&)>* on_done = nullptr;
};

struct SymbolRef {
  uint32_t shard;
  SymbolId local_id;
};

struct ShardedEngineConfig {
  uint32_t shards = 1;
  BookConfig book;
  size_t queue_capacity = 1 << 14;
  // Shard thread spins this long on an empty queue before futex-sleeping.
  std::chrono::microseconds spin_before_sleep{200};
  // Optional CPU pinning: cpu id for shard i, or -1.
  std::vector<int> cpu_affinity;
  // Record queue wait and in-book time per request (two clock reads/op).
  bool measure_latency = false;
};

inline uint64_t now_ns() {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::steady_clock::now().time_since_epoch())
                                   .count());
}

class ShardedEngine {
 public:
  explicit ShardedEngine(ShardedEngineConfig cfg) : cfg_(std::move(cfg)) {
    for (uint32_t i = 0; i < cfg_.shards; ++i) shards_.emplace_back(std::make_unique<Shard>(cfg_));
  }
  ~ShardedEngine() { stop(); }

  // Must be called before start(); not thread-safe.
  SymbolRef add_symbol(const std::string& name) {
    auto it = symbols_.find(name);
    if (it != symbols_.end()) return it->second;
    uint32_t shard = partition_of(name, cfg_.shards);
    SymbolRef ref{shard, shards_[shard]->engine.add_symbol(name)};
    symbols_.emplace(name, ref);
    names_.push_back(name);
    return ref;
  }

  bool lookup(const std::string& name, SymbolRef& out) const {
    auto it = symbols_.find(name);
    if (it == symbols_.end()) return false;
    out = it->second;
    return true;
  }
  const std::vector<std::string>& symbols() const { return names_; }
  uint32_t shard_count() const { return cfg_.shards; }

  void start() {
    if (running_.exchange(true)) return;
    for (uint32_t i = 0; i < cfg_.shards; ++i) {
      shards_[i]->stop.store(false, std::memory_order_release);
      shards_[i]->thread = std::thread([this, i] { run_shard(i); });
      if (i < cfg_.cpu_affinity.size() && cfg_.cpu_affinity[i] >= 0) pin(shards_[i]->thread, cfg_.cpu_affinity[i]);
    }
  }

  void stop() {
    if (!running_.exchange(false)) return;
    for (auto& s : shards_) {
      s->stop.store(true, std::memory_order_release);
      wake(*s);
      if (s->thread.joinable()) s->thread.join();
    }
  }

  // Enqueues a request to the owning shard; blocks (spinning) if the queue
  // is momentarily full. Caller waits on req.completion.
  void submit(uint32_t shard, Request* req) {
    req->completion.reset();
    if (cfg_.measure_latency) req->enqueue_ts_ns = now_ns();
    Shard& s = *shards_[shard];
    while (!s.queue.try_push(req)) __builtin_ia32_pause();
    s.pending.fetch_add(1, std::memory_order_release);
    if (s.sleeping.load(std::memory_order_acquire)) futex_wake(&s.pending, 1);
  }

  // Convenience synchronous call.
  void call(uint32_t shard, Request& req) {
    submit(shard, &req);
    req.completion.wait();
  }

  // Direct (unsynchronized) access for tests/benchmarks when shard threads
  // are NOT running.
  Engine& engine(uint32_t shard) { return shards_[shard]->engine; }

  uint64_t processed(uint32_t shard) const { return shards_[shard]->processed.load(std::memory_order_relaxed); }

 private:
  struct Shard {
    explicit Shard(const ShardedEngineConfig& c) : engine(c.book), queue(c.queue_capacity) {}
    Engine engine;
    MpscQueue<Request*> queue;
    alignas(64) std::atomic<uint32_t> pending{0};
    alignas(64) std::atomic<bool> sleeping{false};
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> processed{0};
    std::thread thread;
  };

  static void pin(std::thread& t, int cpu) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    pthread_setaffinity_np(t.native_handle(), sizeof(set), &set);
  }

  static void wake(Shard& s) {
    s.pending.fetch_add(1, std::memory_order_release);
    futex_wake(&s.pending, 1);
  }

  void run_shard(uint32_t idx) {
    Shard& s = *shards_[idx];
    Request* req;
    auto idle_since = std::chrono::steady_clock::now();
    bool idle = false;
    while (true) {
      if (s.queue.try_pop(req)) {
        idle = false;
        process(s, *req);
        continue;
      }
      if (s.stop.load(std::memory_order_acquire)) return;
      auto now = std::chrono::steady_clock::now();
      if (!idle) { idle = true; idle_since = now; __builtin_ia32_pause(); continue; }
      if (now - idle_since < cfg_.spin_before_sleep) { __builtin_ia32_pause(); continue; }
      // Park: re-check the queue after announcing we sleep to avoid lost wakeups.
      uint32_t seen = s.pending.load(std::memory_order_acquire);
      s.sleeping.store(true, std::memory_order_seq_cst);
      if (s.queue.try_pop(req)) {
        s.sleeping.store(false, std::memory_order_release);
        idle = false;
        process(s, *req);
        continue;
      }
      if (!s.stop.load(std::memory_order_acquire)) futex_wait(&s.pending, seen);
      s.sleeping.store(false, std::memory_order_release);
      idle = false;
    }
  }

  void process(Shard& s, Request& r) {
    uint64_t t0 = 0;
    if (cfg_.measure_latency) { t0 = now_ns(); r.queue_ns = t0 - r.enqueue_ts_ns; }
    switch (r.type) {
      case RequestType::Submit:
        r.trades.clear();
        r.submit_result = s.engine.submit(r.symbol, r.order, r.trades);
        break;
      case RequestType::Cancel:
        r.cancel_result = s.engine.cancel(r.symbol, r.cancel_id);
        break;
      case RequestType::Snapshot:
        r.snapshot = s.engine.has_symbol(r.symbol) ? s.engine.book(r.symbol).snapshot(r.depth) : DepthSnapshot{};
        break;
      case RequestType::BestBidAsk:
        r.bba = s.engine.has_symbol(r.symbol) ? s.engine.book(r.symbol).best_bid_ask() : BestBidAsk{};
        break;
      case RequestType::Shutdown:
        break;
    }
    s.processed.fetch_add(1, std::memory_order_relaxed);
    if (cfg_.measure_latency) { r.done_ts_ns = now_ns(); r.engine_ns = r.done_ts_ns - t0; }
    if (r.on_done) (*r.on_done)(r);
    r.completion.signal();
  }

  ShardedEngineConfig cfg_;
  std::vector<std::unique_ptr<Shard>> shards_;
  std::unordered_map<std::string, SymbolRef> symbols_;
  std::vector<std::string> names_;
  std::atomic<bool> running_{false};
};

}  // namespace me
