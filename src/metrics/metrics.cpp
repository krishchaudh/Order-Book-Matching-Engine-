#include "me/metrics.h"

#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <fstream>
#include <sstream>
#include <thread>

namespace me::metrics {

const std::array<double, Histogram::kBuckets>& Histogram::bounds() {
  static const std::array<double, kBuckets> b = {
      100,     200,     500,     1e3,  2e3,  5e3,  1e4,  2e4,  5e4,  1e5,  2e5,  5e5,  1e6,
      2e6,     5e6,     1e7,     2e7,  5e7,  1e8,  2e8,  5e8,  1e9,  2e9,  5e9,  1e10};
  return b;
}

namespace {

void render_hist(std::ostringstream& os, const char* name, const char* help, const Histogram& h,
                 const std::string& labels) {
  os << "# HELP " << name << ' ' << help << "\n# TYPE " << name << " histogram\n";
  uint64_t cum = 0;
  const auto& b = Histogram::bounds();
  for (int i = 0; i < Histogram::kBuckets; ++i) {
    cum += h.count_at(i);
    char buf[64];
    std::snprintf(buf, sizeof buf, "%g", b[static_cast<size_t>(i)]);
    os << name << "_bucket{" << labels << (labels.empty() ? "" : ",") << "le=\"" << buf << "\"} " << cum << '\n';
  }
  cum += h.count_at(Histogram::kBuckets);
  os << name << "_bucket{" << labels << (labels.empty() ? "" : ",") << "le=\"+Inf\"} " << cum << '\n';
  os << name << "_sum" << (labels.empty() ? "" : "{" + labels + "}") << ' ' << std::fixed << h.sum() << '\n';
  os << name << "_count" << (labels.empty() ? "" : "{" + labels + "}") << ' ' << cum << '\n';
  os.unsetf(std::ios_base::floatfield);
}

}  // namespace

std::string Registry::render() const {
  std::ostringstream os;
  auto counter = [&](const char* name, const char* help, auto get) {
    os << "# HELP " << name << ' ' << help << "\n# TYPE " << name << " counter\n";
    uint64_t total = 0;
    for (size_t i = 0; i < shards_.size(); ++i) {
      uint64_t v = get(*shards_[i]);
      total += v;
      os << name << "{shard=\"" << i << "\"} " << v << '\n';
    }
    os << name << "{shard=\"all\"} " << total << '\n';
  };
  counter("me_orders_received_total", "Orders submitted to the matching engine",
          [](const ShardMetrics& s) { return s.orders_received.value(); });
  counter("me_orders_accepted_total", "Orders accepted (rested, partially or fully filled)",
          [](const ShardMetrics& s) { return s.orders_accepted.value(); });
  counter("me_orders_rejected_total", "Orders rejected by validation",
          [](const ShardMetrics& s) { return s.orders_rejected.value(); });
  counter("me_cancels_received_total", "Cancel requests received",
          [](const ShardMetrics& s) { return s.cancels_received.value(); });
  counter("me_orders_canceled_total", "Orders successfully canceled",
          [](const ShardMetrics& s) { return s.orders_canceled.value(); });
  counter("me_trades_executed_total", "Trades (executions) generated",
          [](const ShardMetrics& s) { return s.trades_executed.value(); });
  counter("me_volume_traded_total", "Quantity traded",
          [](const ShardMetrics& s) { return s.volume_traded.value(); });
  counter("me_operations_total", "Engine operations processed (rate() gives ops/sec)",
          [](const ShardMetrics& s) { return s.operations.value(); });

  os << "# HELP me_active_orders Resting orders currently in the book\n# TYPE me_active_orders gauge\n";
  int64_t active = 0;
  for (size_t i = 0; i < shards_.size(); ++i) {
    active += shards_[i]->active_orders.value();
    os << "me_active_orders{shard=\"" << i << "\"} " << shards_[i]->active_orders.value() << '\n';
  }
  os << "me_active_orders{shard=\"all\"} " << active << '\n';

  for (size_t i = 0; i < shards_.size(); ++i) {
    if (i == 0) {
      os << "# HELP me_matching_latency_ns Time spent inside the order book per operation\n"
            "# TYPE me_matching_latency_ns histogram\n";
    }
    std::ostringstream tmp;
    render_hist(tmp, "me_matching_latency_ns", "", shards_[i]->matching_latency_ns, "shard=\"" + std::to_string(i) + "\"");
    std::string s = tmp.str();
    // strip the duplicate HELP/TYPE header lines emitted by render_hist
    size_t p = s.find('\n'); p = s.find('\n', p + 1);
    os << s.substr(p + 1);
  }

  if (process_) {
    os << "# HELP me_symbols Number of symbols served by this instance\n# TYPE me_symbols gauge\n"
       << "me_symbols " << process_->symbols.value() << '\n';
    os << "# HELP me_grpc_requests_total gRPC requests handled\n# TYPE me_grpc_requests_total counter\n"
       << "me_grpc_requests_total " << process_->grpc_requests.value() << '\n';
    os << "# HELP me_grpc_errors_total gRPC requests that returned an error status\n# TYPE me_grpc_errors_total counter\n"
       << "me_grpc_errors_total " << process_->grpc_errors.value() << '\n';
    render_hist(os, "me_grpc_latency_ns", "Server-side gRPC handler latency", process_->grpc_latency_ns, "");
    render_hist(os, "me_queue_latency_ns", "Request enqueue to shard pickup latency", process_->queue_latency_ns, "");
  }

  ResourceUsage ru = read_resource_usage();
  os << "# HELP process_cpu_seconds_total Total user and system CPU time\n# TYPE process_cpu_seconds_total counter\n"
     << "process_cpu_seconds_total " << ru.cpu_seconds_total << '\n'
     << "# HELP process_resident_memory_bytes Resident memory size\n# TYPE process_resident_memory_bytes gauge\n"
     << "process_resident_memory_bytes " << ru.rss_bytes << '\n'
     << "# HELP process_virtual_memory_bytes Virtual memory size\n# TYPE process_virtual_memory_bytes gauge\n"
     << "process_virtual_memory_bytes " << ru.vms_bytes << '\n'
     << "# HELP process_open_fds Open file descriptors\n# TYPE process_open_fds gauge\n"
     << "process_open_fds " << ru.open_fds << '\n';

  std::function<std::string()> extra;
  {
    std::lock_guard<std::mutex> g(mu_);
    extra = extra_;
  }
  if (extra) os << extra();
  return os.str();
}

ResourceUsage read_resource_usage() {
  ResourceUsage ru{0, 0, 0, 0};
  std::ifstream stat("/proc/self/stat");
  std::string line;
  if (std::getline(stat, line)) {
    // Fields after the ")" are space separated; utime is field 14, stime 15, vsize 23, rss 24.
    size_t p = line.rfind(')');
    std::istringstream is(line.substr(p + 2));
    std::string tok;
    std::vector<std::string> f;
    while (is >> tok) f.push_back(tok);
    if (f.size() > 22) {
      long ticks = sysconf(_SC_CLK_TCK);
      ru.cpu_seconds_total = (std::stod(f[11]) + std::stod(f[12])) / static_cast<double>(ticks);
      ru.vms_bytes = std::stoll(f[20]);
      ru.rss_bytes = std::stoll(f[21]) * sysconf(_SC_PAGESIZE);
    }
  }
  if (DIR* d = opendir("/proc/self/fd")) {
    while (readdir(d)) ++ru.open_fds;
    ru.open_fds -= 3;  // ".", "..", and the dir fd itself
    closedir(d);
  }
  return ru;
}

}  // namespace me::metrics
