// Minimal, dependency-free Prometheus instrumentation.
//
// Hot-path cost model: counters and histograms are plain relaxed atomics that
// are only ever *written* by one shard thread each (per-shard instances), so
// an increment is a single uncontended cache-local store. The exporter thread
// reads them with relaxed loads. No locks, no allocation, no string handling
// on the matching path. Latency histograms use fixed log-spaced buckets.
#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace me::metrics {

class Counter {
 public:
  void inc(uint64_t n = 1) { v_.store(v_.load(std::memory_order_relaxed) + n, std::memory_order_relaxed); }
  void add_atomic(uint64_t n = 1) { v_.fetch_add(n, std::memory_order_relaxed); }
  uint64_t value() const { return v_.load(std::memory_order_relaxed); }

 private:
  std::atomic<uint64_t> v_{0};
};

class Gauge {
 public:
  void set(int64_t v) { v_.store(v, std::memory_order_relaxed); }
  void add(int64_t d) { v_.fetch_add(d, std::memory_order_relaxed); }
  int64_t value() const { return v_.load(std::memory_order_relaxed); }

 private:
  std::atomic<int64_t> v_{0};
};

// Latency histogram in nanoseconds; 1-2-5 buckets from 100ns to 10s.
class Histogram {
 public:
  static constexpr int kBuckets = 25;
  static const std::array<double, kBuckets>& bounds();

  void observe(double v) {
    int i = 0;
    const auto& b = bounds();
    while (i < kBuckets && v > b[static_cast<size_t>(i)]) ++i;
    counts_[static_cast<size_t>(i)].store(counts_[static_cast<size_t>(i)].load(std::memory_order_relaxed) + 1,
                                          std::memory_order_relaxed);
    sum_.store(sum_.load(std::memory_order_relaxed) + v, std::memory_order_relaxed);
  }
  // Thread-safe variant for histograms shared by several writer threads
  // (gRPC handler threads); the plain observe() is for single-writer shards.
  void observe_atomic(double v) {
    int i = 0;
    const auto& b = bounds();
    while (i < kBuckets && v > b[static_cast<size_t>(i)]) ++i;
    counts_[static_cast<size_t>(i)].fetch_add(1, std::memory_order_relaxed);
    double cur = sum_.load(std::memory_order_relaxed);
    while (!sum_.compare_exchange_weak(cur, cur + v, std::memory_order_relaxed)) {}
  }
  uint64_t count_at(int i) const { return counts_[static_cast<size_t>(i)].load(std::memory_order_relaxed); }
  double sum() const { return sum_.load(std::memory_order_relaxed); }
  uint64_t total() const {
    uint64_t t = 0;
    for (auto& c : counts_) t += c.load(std::memory_order_relaxed);
    return t;
  }

 private:
  std::array<std::atomic<uint64_t>, kBuckets + 1> counts_{};
  std::atomic<double> sum_{0.0};
};

// Per-shard engine metrics. One instance per shard thread => single writer.
struct alignas(64) ShardMetrics {
  Counter orders_received;
  Counter orders_accepted;   // rested or (partially) filled
  Counter orders_rejected;
  Counter cancels_received;
  Counter orders_canceled;   // successful cancels
  Counter trades_executed;
  Counter volume_traded;
  Counter operations;        // all engine operations processed
  Histogram matching_latency_ns;  // time inside OrderBook::submit/cancel
  Gauge active_orders;
};

struct ProcessMetrics {
  Gauge symbols;
  Counter grpc_requests;
  Counter grpc_errors;
  Histogram grpc_latency_ns;     // server-side handler latency
  Histogram queue_latency_ns;    // enqueue -> shard thread pickup
};

// Prometheus text exposition for the whole process.
class Registry {
 public:
  Registry(std::vector<ShardMetrics*> shards, ProcessMetrics* process)
      : shards_(std::move(shards)), process_(process) {}

  // Optional extra text appended on every scrape (e.g. book depth gauges).
  void set_extra_provider(std::function<std::string()> f) {
    std::lock_guard<std::mutex> g(mu_);
    extra_ = std::move(f);
  }

  std::string render() const;

 private:
  std::vector<ShardMetrics*> shards_;
  ProcessMetrics* process_;
  mutable std::mutex mu_;
  std::function<std::string()> extra_;
};

// Tiny HTTP/1.1 server exposing /metrics, /healthz and /readyz.
class HttpExporter {
 public:
  HttpExporter(const Registry& reg, uint16_t port, std::function<bool()> ready);
  ~HttpExporter();
  bool start();  // returns false if the port could not be bound
  void stop();
  uint16_t port() const { return port_; }

 private:
  void serve();
  const Registry& reg_;
  uint16_t port_;
  std::function<bool()> ready_;
  int listen_fd_ = -1;
  std::atomic<bool> stop_{false};
  std::unique_ptr<std::thread> thread_;
};

// Reads /proc/self for utilisation metrics (cheap; called on scrape only).
struct ResourceUsage {
  double cpu_seconds_total;
  int64_t rss_bytes;
  int64_t vms_bytes;
  int64_t open_fds;
};
ResourceUsage read_resource_usage();

}  // namespace me::metrics
