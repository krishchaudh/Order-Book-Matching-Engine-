// gRPC load tester. Measures what a *client* observes end-to-end (serialize,
// HTTP/2, server handler, shard round trip, response) — never the internal
// matching throughput.
//
//   me_loadtest [--target=HOST:PORT] [--mode=unary|stream] [--concurrency=1,4,16,64]
//               [--duration=10] [--warmup=2] [--symbols=AAPL,MSFT] [--channels=4]
//               [--inflight=32] [--mix=60,25,15] [--metrics=HOST:9100] [--json=FILE]
//               [--label=TEXT]
//
// unary : each client thread issues SubmitOrder/CancelOrder RPCs back to back
//         (closed loop, 1 in flight per client).
// stream: each client thread owns one bidirectional SubmitOrderStream with up
//         to --inflight requests outstanding (pipelined; tests batching).
// Every request carries a real order: passive limit, cancel of a live order,
// or crossing limit/market order, so the server does real matching work.
#include <arpa/inet.h>
#include <grpcpp/grpcpp.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "matching_engine.grpc.pb.h"

namespace mev1 = me::v1;
using Clock = std::chrono::steady_clock;

struct Options {
  std::string target = "localhost:50051";
  std::string mode = "unary";
  std::vector<int> concurrency = {1, 4, 16, 64};
  double duration = 10, warmup = 2;
  std::vector<std::string> symbols = {"AAPL", "MSFT", "GOOG", "AMZN"};
  int channels = 4;
  int inflight = 32;
  int mix_passive = 60, mix_cancel = 25, mix_aggress = 15;
  std::string metrics;
  std::string json;
  std::string label;
};

static std::vector<std::string> split(const std::string& s, char d) {
  std::vector<std::string> out; std::stringstream ss(s); std::string it;
  while (std::getline(ss, it, d)) if (!it.empty()) out.push_back(it);
  return out;
}

static Options parse(int argc, char** argv) {
  Options o;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto val = [&](const char* k) -> const char* { size_t n = std::strlen(k); return a.compare(0, n, k) == 0 && a[n] == '=' ? a.c_str() + n + 1 : nullptr; };
    const char* v;
    if ((v = val("--target"))) o.target = v;
    else if ((v = val("--mode"))) o.mode = v;
    else if ((v = val("--concurrency"))) { o.concurrency.clear(); for (auto& c : split(v, ',')) o.concurrency.push_back(std::atoi(c.c_str())); }
    else if ((v = val("--duration"))) o.duration = std::atof(v);
    else if ((v = val("--warmup"))) o.warmup = std::atof(v);
    else if ((v = val("--symbols"))) o.symbols = split(v, ',');
    else if ((v = val("--channels"))) o.channels = std::atoi(v);
    else if ((v = val("--inflight"))) o.inflight = std::atoi(v);
    else if ((v = val("--mix"))) std::sscanf(v, "%d,%d,%d", &o.mix_passive, &o.mix_cancel, &o.mix_aggress);
    else if ((v = val("--metrics"))) o.metrics = v;
    else if ((v = val("--json"))) o.json = v;
    else if ((v = val("--label"))) o.label = v;
    else { std::fprintf(stderr, "unknown option %s (see header of loadtest.cpp)\n", a.c_str()); std::exit(2); }
  }
  return o;
}

// ------------------------------------------------- server resource sampling --
struct ServerSample { double cpu_seconds = -1; double rss_bytes = -1; bool ok = false; };

static ServerSample scrape(const std::string& hostport) {
  ServerSample s;
  if (hostport.empty()) return s;
  auto parts = split(hostport, ':');
  if (parts.size() != 2) return s;
  addrinfo hints{}; hints.ai_socktype = SOCK_STREAM;
  addrinfo* res = nullptr;
  if (getaddrinfo(parts[0].c_str(), parts[1].c_str(), &hints, &res) != 0) return s;
  int fd = -1;
  for (addrinfo* p = res; p; p = p->ai_next) {
    fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (fd >= 0 && connect(fd, p->ai_addr, p->ai_addrlen) == 0) break;
    if (fd >= 0) close(fd);
    fd = -1;
  }
  freeaddrinfo(res);
  if (fd < 0) return s;
  std::string req = "GET /metrics HTTP/1.0\r\nHost: " + parts[0] + "\r\n\r\n";
  if (write(fd, req.data(), req.size()) < 0) { close(fd); return s; }
  std::string body;
  char buf[4096];
  ssize_t n;
  while ((n = read(fd, buf, sizeof buf)) > 0) body.append(buf, static_cast<size_t>(n));
  close(fd);
  std::istringstream is(body);
  std::string line;
  while (std::getline(is, line)) {
    if (line.rfind("process_cpu_seconds_total ", 0) == 0) { s.cpu_seconds = std::atof(line.c_str() + 26); s.ok = true; }
    else if (line.rfind("process_resident_memory_bytes ", 0) == 0) s.rss_bytes = std::atof(line.c_str() + 30);
  }
  return s;
}

// ------------------------------------------------------------- order flow ----
class Flow {
 public:
  // Order ids must be unique per symbol for the life of the server: encode a
  // per-run nonce so successive concurrency levels / runs never collide.
  Flow(uint64_t client_idx, const Options& o)
      : o_(o), rng_(1000 + client_idx),
        base_id_(((static_cast<uint64_t>(Clock::now().time_since_epoch().count()) >> 20) & 0xFFFFFull) << 42 | (client_idx & 0x3FF) << 32) {}

  // Fills either a submit or a cancel; returns true for submit.
  bool next(mev1::SubmitOrderRequest& sub, mev1::CancelOrderRequest& can) {
    const std::string& sym = o_.symbols[rng_() % o_.symbols.size()];
    int roll = static_cast<int>(rng_() % 100);
    if (roll < o_.mix_cancel && !live_.empty()) {
      size_t k = rng_() % live_.size();
      can.set_symbol(live_[k].second);
      can.set_order_id(live_[k].first);
      live_[k] = live_.back(); live_.pop_back();
      return false;
    }
    uint64_t id = base_id_ + (++seq_);
    sub.set_symbol(sym); sub.set_order_id(id);
    bool buy = rng_() & 1;
    sub.set_side(buy ? mev1::BUY : mev1::SELL);
    if (roll >= 100 - o_.mix_aggress) {
      bool market = (rng_() % 4) == 0;
      sub.set_type(market ? mev1::MARKET : mev1::LIMIT);
      sub.set_price(market ? 0 : (buy ? 10000 + 60 : 10000 - 60));  // crosses the whole passive band
      sub.set_quantity(1 + static_cast<uint32_t>(rng_() % 20));
    } else {
      sub.set_type(mev1::LIMIT);
      int64_t off = 1 + static_cast<int64_t>(rng_() % 50);
      sub.set_price(buy ? 10000 - off : 10000 + off);
      sub.set_quantity(1 + static_cast<uint32_t>(rng_() % 100));
      live_.emplace_back(id, sym);
      if (live_.size() > 20000) { live_[rng_() % live_.size()] = live_.back(); live_.pop_back(); }
    }
    return true;
  }

 private:
  const Options& o_;
  std::mt19937_64 rng_;
  uint64_t base_id_;
  uint64_t seq_ = 0;
  std::vector<std::pair<uint64_t, std::string>> live_;
};

struct ClientStats {
  std::vector<uint32_t> lat_ns;  // measurement window only
  uint64_t total = 0, ok = 0, rpc_errors = 0, rejected = 0, fills = 0;
};

// ------------------------------------------------------------- unary mode ----
static void run_unary(int idx, mev1::MatchingEngine::Stub& stub, const Options& o, std::atomic<int>& phase, ClientStats& st) {
  Flow flow(static_cast<uint64_t>(idx), o);
  mev1::SubmitOrderRequest sub; mev1::CancelOrderRequest can;
  mev1::ExecutionReport rep; mev1::CancelOrderResponse crep;
  st.lat_ns.reserve(1 << 20);
  while (phase.load(std::memory_order_relaxed) < 3) {
    bool is_sub = flow.next(sub, can);
    grpc::ClientContext ctx;
    auto t0 = Clock::now();
    grpc::Status s = is_sub ? stub.SubmitOrder(&ctx, sub, &rep) : stub.CancelOrder(&ctx, can, &crep);
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
    if (phase.load(std::memory_order_relaxed) != 2) continue;  // warmup / cooldown not counted
    st.total++;
    if (!s.ok()) { st.rpc_errors++; continue; }
    st.ok++;
    st.lat_ns.push_back(static_cast<uint32_t>(std::min<long>(ns, 0xFFFFFFFF)));
    if (is_sub) { if (rep.status() == mev1::REJECTED) st.rejected++; st.fills += static_cast<uint64_t>(rep.fills_size()); }
  }
}

// ------------------------------------------------------------ stream mode ----
static void run_stream(int idx, mev1::MatchingEngine::Stub& stub, const Options& o, std::atomic<int>& phase, ClientStats& st) {
  Flow flow(static_cast<uint64_t>(idx), o);
  grpc::ClientContext ctx;
  auto stream = stub.SubmitOrderStream(&ctx);
  const size_t W = static_cast<size_t>(std::max(1, o.inflight));
  std::vector<Clock::time_point> sent_at(W);
  std::atomic<uint64_t> sent{0}, recv{0};
  st.lat_ns.reserve(1 << 20);
  std::atomic<bool> reader_done{false};
  std::thread reader([&] {
    mev1::ExecutionReport rep;
    while (stream->Read(&rep)) {
      uint64_t k = recv.load(std::memory_order_relaxed);
      auto t0 = sent_at[k % W];
      auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
      recv.store(k + 1, std::memory_order_release);
      if (phase.load(std::memory_order_relaxed) != 2) continue;
      st.total++; st.ok++;
      st.lat_ns.push_back(static_cast<uint32_t>(std::min<long>(ns, 0xFFFFFFFF)));
      if (rep.status() == mev1::REJECTED) st.rejected++;
      st.fills += static_cast<uint64_t>(rep.fills_size());
    }
    reader_done.store(true);
  });
  mev1::SubmitOrderRequest sub; mev1::CancelOrderRequest can;
  while (phase.load(std::memory_order_relaxed) < 3) {
    while (!flow.next(sub, can)) {}  // stream carries submits only
    uint64_t k = sent.load(std::memory_order_relaxed);
    while (k - recv.load(std::memory_order_acquire) >= W) {
      if (phase.load(std::memory_order_relaxed) >= 3) break;
      std::this_thread::yield();
    }
    sent_at[k % W] = Clock::now();
    sent.store(k + 1, std::memory_order_release);
    if (!stream->Write(sub)) break;
  }
  stream->WritesDone();
  reader.join();
  grpc::Status s = stream->Finish();
  if (!s.ok()) st.rpc_errors++;
}

// ----------------------------------------------------------------- report ----
struct Row {
  int concurrency; double rps, ok_rps, err_rate, p50, p90, p95, p99, p999, max, mean; uint64_t n, rejected, fills;
  double server_cpu = -1, server_rss_mb = -1;
};

static double pct(std::vector<uint32_t>& v, double p) {
  if (v.empty()) return 0;
  size_t k = static_cast<size_t>(p * static_cast<double>(v.size() - 1));
  std::nth_element(v.begin(), v.begin() + static_cast<std::ptrdiff_t>(k), v.end());
  return v[k];
}

static Row run_level(const Options& o, int conc) {
  std::vector<std::shared_ptr<grpc::Channel>> channels;
  for (int i = 0; i < std::max(1, o.channels); ++i) {
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);  // distinct TCP connections
    channels.push_back(grpc::CreateCustomChannel(o.target, grpc::InsecureChannelCredentials(), args));
  }
  std::vector<std::unique_ptr<mev1::MatchingEngine::Stub>> stubs;
  for (int i = 0; i < conc; ++i) stubs.push_back(mev1::MatchingEngine::NewStub(channels[static_cast<size_t>(i) % channels.size()]));
  std::vector<ClientStats> stats(static_cast<size_t>(conc));
  std::atomic<int> phase{1};  // 1 warmup, 2 measure, 3 stop
  std::vector<std::thread> threads;
  for (int i = 0; i < conc; ++i)
    threads.emplace_back([&, i] {
      if (o.mode == "stream") run_stream(i, *stubs[static_cast<size_t>(i)], o, phase, stats[static_cast<size_t>(i)]);
      else run_unary(i, *stubs[static_cast<size_t>(i)], o, phase, stats[static_cast<size_t>(i)]);
    });
  std::this_thread::sleep_for(std::chrono::duration<double>(o.warmup));
  ServerSample s0 = scrape(o.metrics);
  auto t0 = Clock::now();
  phase.store(2);
  std::this_thread::sleep_for(std::chrono::duration<double>(o.duration));
  phase.store(3);
  auto t1 = Clock::now();
  ServerSample s1 = scrape(o.metrics);
  for (auto& t : threads) t.join();

  Row r{};
  r.concurrency = conc;
  std::vector<uint32_t> all;
  uint64_t total = 0, ok = 0, errs = 0;
  double sum = 0;
  for (auto& s : stats) {
    total += s.total; ok += s.ok; errs += s.rpc_errors; r.rejected += s.rejected; r.fills += s.fills;
    all.insert(all.end(), s.lat_ns.begin(), s.lat_ns.end());
  }
  for (uint32_t v : all) sum += v;
  double secs = std::chrono::duration<double>(t1 - t0).count();
  r.n = total; r.rps = total / secs; r.ok_rps = ok / secs; r.err_rate = total ? static_cast<double>(errs) / total : 0;
  r.mean = all.empty() ? 0 : sum / all.size();
  r.p50 = pct(all, 0.5); r.p90 = pct(all, 0.9); r.p95 = pct(all, 0.95); r.p99 = pct(all, 0.99); r.p999 = pct(all, 0.999);
  r.max = all.empty() ? 0 : *std::max_element(all.begin(), all.end());
  if (s0.ok && s1.ok) { r.server_cpu = (s1.cpu_seconds - s0.cpu_seconds) / secs * 100.0; r.server_rss_mb = s1.rss_bytes / 1048576.0; }
  return r;
}

int main(int argc, char** argv) {
  Options o = parse(argc, argv);
  std::printf("# target=%s mode=%s channels=%d inflight=%d duration=%.1fs warmup=%.1fs symbols=%zu mix=%d/%d/%d %s\n", o.target.c_str(),
              o.mode.c_str(), o.channels, o.inflight, o.duration, o.warmup, o.symbols.size(), o.mix_passive, o.mix_cancel, o.mix_aggress, o.label.c_str());
  std::printf("%5s %10s %10s %7s | %8s %8s %8s %8s %8s %8s %9s | %7s %8s | %9s %9s\n", "conc", "req/s", "ok/s", "err%", "mean_us", "p50_us", "p90_us",
              "p95_us", "p99_us", "p99.9us", "max_us", "rej%", "fills", "srv_cpu%", "srv_rss");
  for (int c : o.concurrency) {
    Row r = run_level(o, c);
    std::printf("%5d %10.0f %10.0f %7.3f | %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f %9.1f | %7.2f %8lu | %9.0f %9.1f\n", r.concurrency, r.rps, r.ok_rps,
                r.err_rate * 100, r.mean / 1e3, r.p50 / 1e3, r.p90 / 1e3, r.p95 / 1e3, r.p99 / 1e3, r.p999 / 1e3, r.max / 1e3,
                r.n ? 100.0 * static_cast<double>(r.rejected) / static_cast<double>(r.n) : 0.0, static_cast<unsigned long>(r.fills), r.server_cpu, r.server_rss_mb);
    std::fflush(stdout);
    if (!o.json.empty()) {
      std::ofstream f(o.json, std::ios::app);
      f << "{\"label\":\"" << o.label << "\",\"target\":\"" << o.target << "\",\"mode\":\"" << o.mode << "\",\"concurrency\":" << c
        << ",\"channels\":" << o.channels << ",\"inflight\":" << o.inflight << ",\"duration_s\":" << o.duration << ",\"requests\":" << r.n
        << ",\"rps\":" << r.rps << ",\"ok_rps\":" << r.ok_rps << ",\"error_rate\":" << r.err_rate << ",\"mean_us\":" << r.mean / 1e3
        << ",\"p50_us\":" << r.p50 / 1e3 << ",\"p90_us\":" << r.p90 / 1e3 << ",\"p95_us\":" << r.p95 / 1e3 << ",\"p99_us\":" << r.p99 / 1e3
        << ",\"p999_us\":" << r.p999 / 1e3 << ",\"max_us\":" << r.max / 1e3 << ",\"rejected\":" << r.rejected << ",\"fills\":" << r.fills
        << ",\"server_cpu_pct\":" << r.server_cpu << ",\"server_rss_mb\":" << r.server_rss_mb << "}\n";
    }
  }
  return 0;
}
