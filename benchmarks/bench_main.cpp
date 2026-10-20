// Standalone benchmark harness for the matching engine.
//
// Every counted operation is a real submit/cancel against a live order book.
// Operation streams are generated *before* timing by replaying them through
// a shadow engine, which also records the expected outcome (trade count,
// trade checksum, open orders) so each timed trial can be validated: the
// optimized engine must reproduce exactly the same executions.
//
// Usage: me_bench [--workload=NAME|all] [--book=dense|map|sortedvec|dense_std|map_oa|reference]
//                 [--ops=N] [--trials=K] [--latency] [--threads=T] [--symbols=S]
//                 [--prefill=N] [--spread=TICKS] [--seed=S] [--pin[=CPU]] [--json=FILE]
//                 [--mix=insert,cancel,aggress]  (percentages, e.g. 60,30,10)
#include <sched.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "me/engine.h"
#include "me/order_book.h"
#include "me/reference_book.h"
#include "me/sharded_engine.h"

using namespace me;
using Clock = std::chrono::steady_clock;

// ------------------------------------------------------------ allocation ----
static std::atomic<uint64_t> g_allocs{0};
static thread_local bool t_count_allocs = false;
void* operator new(size_t n) {
  if (t_count_allocs) g_allocs.fetch_add(1, std::memory_order_relaxed);
  if (void* p = std::malloc(n ? n : 1)) return p;
  throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, size_t) noexcept { std::free(p); }

// --------------------------------------------------------------- options ----
struct Options {
  std::string workload = "all";
  std::string book = "dense";
  size_t ops = 5'000'000;
  int trials = 5;
  bool latency = false;
  int threads = 4;
  int symbols = 1;
  size_t prefill = 100'000;
  Price spread = 50;
  uint64_t seed = 42;
  int pin = -1;
  bool pin_set = false;
  std::string json;
  int mix_insert = 60, mix_cancel = 30, mix_aggress = 10;
  bool quiet = false;
  bool scramble_ids = false;  // --ids=scrambled: bijectively scrambled (random-looking) order ids
};

static Options parse(int argc, char** argv) {
  Options o;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto val = [&](const char* k) -> const char* {
      size_t n = std::strlen(k);
      return a.compare(0, n, k) == 0 && a[n] == '=' ? a.c_str() + n + 1 : nullptr;
    };
    const char* v;
    if ((v = val("--workload"))) o.workload = v;
    else if ((v = val("--book"))) o.book = v;
    else if ((v = val("--ops"))) o.ops = std::strtoull(v, nullptr, 10);
    else if ((v = val("--trials"))) o.trials = std::atoi(v);
    else if (a == "--latency") o.latency = true;
    else if ((v = val("--threads"))) o.threads = std::atoi(v);
    else if ((v = val("--symbols"))) o.symbols = std::atoi(v);
    else if ((v = val("--prefill"))) o.prefill = std::strtoull(v, nullptr, 10);
    else if ((v = val("--spread"))) o.spread = std::atoll(v);
    else if ((v = val("--seed"))) o.seed = std::strtoull(v, nullptr, 10);
    else if (a == "--pin") { o.pin_set = true; o.pin = 2; }
    else if ((v = val("--pin"))) { o.pin_set = true; o.pin = std::atoi(v); }
    else if ((v = val("--json"))) o.json = v;
    else if (a == "--quiet") o.quiet = true;
    else if ((v = val("--ids"))) o.scramble_ids = std::string(v) == "scrambled";
    else if ((v = val("--mix"))) { std::sscanf(v, "%d,%d,%d", &o.mix_insert, &o.mix_cancel, &o.mix_aggress); }
    else if (a == "--help" || a == "-h") {
      std::puts("see header of benchmarks/bench_main.cpp for options");
      std::exit(0);
    } else {
      std::fprintf(stderr, "unknown option %s\n", a.c_str());
      std::exit(2);
    }
  }
  return o;
}

// ------------------------------------------------------------ op streams ----
struct Op {
  uint8_t kind;  // 0 submit, 1 cancel
  Side side;
  OrderType type;
  SymbolId symbol;
  OrderId id;
  Price price;
  Qty qty;
};

struct Expected {
  uint64_t trades = 0;
  uint64_t checksum = 0;  // order-sensitive hash over (seq, maker, taker, px, qty)
  uint64_t open_orders = 0;
  uint64_t fills_volume = 0;
};

static inline uint64_t mix64(uint64_t h, uint64_t v) {
  h ^= v + 0x9E3779B97F4A7C15ull + (h << 6) + (h >> 2);
  return h;
}
static inline void fold_trades(Expected& e, const std::vector<Trade>& trades) {
  for (const Trade& t : trades) {
    e.checksum = mix64(e.checksum, t.seq);
    e.checksum = mix64(e.checksum, t.maker_id);
    e.checksum = mix64(e.checksum, t.taker_id);
    e.checksum = mix64(e.checksum, static_cast<uint64_t>(t.price));
    e.checksum = mix64(e.checksum, t.qty);
    e.fills_volume += t.qty;
  }
  e.trades += trades.size();
}

struct Stream {
  std::vector<Op> prefill;  // applied untimed before every trial
  std::vector<Op> ops;      // timed
  Expected expected;        // state after prefill + ops
  std::string description;
};

// Generator that keeps a shadow engine so cancels target live orders and
// aggressive orders really cross.

// Pure insert workloads can leave every op resting, so size the pool for the
// run instead of relying on the 4M default (pages are committed lazily).
static BookConfig book_config(const Options& o) {
  BookConfig c;
  uint64_t need = static_cast<uint64_t>(o.prefill) + o.ops + 1024;
  if (need > c.max_open_orders) c.max_open_orders = static_cast<uint32_t>(std::min<uint64_t>(need, (1ull << 31) - 1));
  return c;
}

class Generator {
 public:
  Generator(const Options& o, uint64_t seed, int symbols) : o_(o), rng_(seed), shadow_(book_config(o)) {
    for (int s = 0; s < symbols; ++s) {
      shadow_.add_symbol("S" + std::to_string(s));
      mid_.push_back(10'000 + 100 * s);
    }
    alive_.resize(1, 0);
  }

  Stream insert_stream(size_t n) {
    Stream s;
    s.description = "limit inserts, non-crossing, " + std::to_string(2 * o_.spread) + " price levels";
    for (size_t i = 0; i < n; ++i) push(s.ops, make_passive(static_cast<SymbolId>(i % mid_.size())));
    finish(s);
    return s;
  }

  Stream cancel_stream(size_t n) {
    Stream s;
    s.description = "cancel of resting orders in random order";
    for (size_t i = 0; i < n; ++i) push(s.prefill, make_passive(static_cast<SymbolId>(i % mid_.size())));
    std::vector<OrderId> ids(live_.begin(), live_.end());
    std::shuffle(ids.begin(), ids.end(), rng_);
    for (OrderId id : ids) push(s.ops, Op{1, Side::Buy, OrderType::Limit, symbol_of(id), id, 0, 0});
    finish(s);
    return s;
  }

  // Each aggressive order fully fills exactly one resting order (qty match).
  Stream match_stream(size_t n) {
    Stream s;
    s.description = "aggressive limit orders, each fully filling one resting order";
    for (size_t i = 0; i < n; ++i) push(s.prefill, make_passive(static_cast<SymbolId>(i % mid_.size()), 10));
    for (size_t i = 0; i < n; ++i) {
      SymbolId sym = static_cast<SymbolId>(i % mid_.size());
      auto bba = shadow_.book(sym).best_bid_ask();
      if (!bba.has_bid && !bba.has_ask) break;
      bool hit_ask = bba.has_ask && (!bba.has_bid || (rng_() & 1));
      Price px = hit_ask ? bba.ask_price : bba.bid_price;
      push(s.ops, Op{0, hit_ask ? Side::Buy : Side::Sell, OrderType::Limit, sym, next_id(), px, 10});
    }
    finish(s);
    return s;
  }

  // Large resting orders nibbled by small aggressive orders: every op is a
  // partial fill of the resting order (maker remains, taker fully filled).
  Stream partial_stream(size_t n) {
    Stream s;
    s.description = "small aggressive orders partially filling large resting orders";
    size_t resting = std::max<size_t>(1000, n / 100);
    for (size_t i = 0; i < resting; ++i) push(s.prefill, make_passive(static_cast<SymbolId>(i % mid_.size()), 1000));
    for (size_t i = 0; i < n; ++i) {
      SymbolId sym = static_cast<SymbolId>(i % mid_.size());
      auto bba = shadow_.book(sym).best_bid_ask();
      if (!bba.has_bid && !bba.has_ask) break;
      bool hit_ask = bba.has_ask && (!bba.has_bid || (rng_() & 1));
      Price px = hit_ask ? bba.ask_price : bba.bid_price;
      push(s.ops, Op{0, hit_ask ? Side::Buy : Side::Sell, OrderType::Limit, sym, next_id(), px, 7});
    }
    finish(s);
    return s;
  }

  // Steady-state mixed flow around a prefilled book.
  Stream mixed_stream(size_t n, size_t prefill, int market_pct = 20) {
    Stream s;
    std::ostringstream d;
    d << "mixed " << o_.mix_insert << "% passive insert / " << o_.mix_cancel << "% cancel / " << o_.mix_aggress
      << "% aggressive (" << market_pct << "% of those market), prefill " << prefill << ", spread " << o_.spread;
    s.description = d.str();
    for (size_t i = 0; i < prefill; ++i) push(s.prefill, make_passive(static_cast<SymbolId>(i % mid_.size())));
    for (size_t i = 0; i < n; ++i) {
      SymbolId sym = static_cast<SymbolId>(rng_() % mid_.size());
      int roll = static_cast<int>(rng_() % 100);
      if (roll < o_.mix_cancel) {
        OrderId id = pick_live();
        if (id != 0) { push(s.ops, Op{1, Side::Buy, OrderType::Limit, symbol_of(id), id, 0, 0}); continue; }
        roll = o_.mix_cancel;  // nothing to cancel: fall through to insert
      }
      if (roll < o_.mix_cancel + o_.mix_aggress) {
        auto bba = shadow_.book(sym).best_bid_ask();
        if (bba.has_bid || bba.has_ask) {
          bool hit_ask = bba.has_ask && (!bba.has_bid || (rng_() & 1));
          bool market = static_cast<int>(rng_() % 100) < market_pct;
          Price px = hit_ask ? bba.ask_price + static_cast<Price>(rng_() % 3) : bba.bid_price - static_cast<Price>(rng_() % 3);
          if (px < 1) px = 1;
          Qty q = static_cast<Qty>(1 + rng_() % 150);
          push(s.ops, Op{0, hit_ask ? Side::Buy : Side::Sell, market ? OrderType::Market : OrderType::Limit, sym,
                         next_id(), market ? 0 : px, q});
          continue;
        }
      }
      push(s.ops, make_passive(sym));
      if ((i & 1023) == 0) drift();
    }
    finish(s);
    return s;
  }

 private:
  Op make_passive(SymbolId sym, Qty fixed_qty = 0) {
    auto bba = shadow_.book(sym).best_bid_ask();
    Price mid = mid_[sym];
    // Stay strictly inside the touch so the order rests.
    Price off = 1 + static_cast<Price>(rng_() % static_cast<uint64_t>(o_.spread));
    Side side = (rng_() & 1) ? Side::Buy : Side::Sell;
    Price px = side == Side::Buy ? mid - off : mid + off;
    if (side == Side::Buy && bba.has_ask && px >= bba.ask_price) px = bba.ask_price - 1;
    if (side == Side::Sell && bba.has_bid && px <= bba.bid_price) px = bba.bid_price + 1;
    if (px < 1) px = 1;
    Qty q = fixed_qty ? fixed_qty : static_cast<Qty>(1 + rng_() % 100);
    return Op{0, side, OrderType::Limit, sym, next_id(), px, q};
  }

  void drift() {
    for (auto& m : mid_) {
      m += static_cast<Price>(rng_() % 3) - 1;
      if (m < o_.spread + 2) m = o_.spread + 2;
    }
  }

  // Sequential ids model gateway-assigned ids; "scrambled" ids are a bijective
  // permutation of them (multiply by an odd constant mod 2^bits) so the id
  // space is identical in size but has no locality, stressing the id index.
  OrderId next_id() {
    OrderId k = ++last_id_;
    if (!o_.scramble_ids) return k;
    if (id_mask_ == 0) {
      uint64_t need = static_cast<uint64_t>(o_.prefill) + o_.ops + 2;
      uint64_t cap = 1;
      while (cap < need) cap <<= 1;
      id_mask_ = cap - 1;
    }
    return ((k * 0x9E3779B97F4A7C15ull) & id_mask_) + 1;
  }
  uint64_t id_mask_ = 0;
  SymbolId symbol_of(OrderId id) const { return id_symbol_[id]; }

  OrderId pick_live() {
    while (!live_.empty()) {
      size_t k = rng_() % live_.size();
      OrderId id = live_[k];
      if (alive_[id]) return id;
      live_[k] = live_.back();
      live_.pop_back();
    }
    return 0;
  }

  void push(std::vector<Op>& out, Op op) {
    out.push_back(op);
    apply(op);
  }

  void apply(const Op& op) {
    if (alive_.size() <= op.id) { alive_.resize((op.id + 1) * 2, 0); id_symbol_.resize((op.id + 1) * 2, 0); }
    trades_.clear();
    if (op.kind == 0) {
      id_symbol_[op.id] = op.symbol;
      auto r = shadow_.submit(op.symbol, NewOrder{op.id, op.price, op.qty, op.side, op.type}, trades_);
      if (r.status == OrderStatus::Rejected) { std::fprintf(stderr, "generator produced rejected order (%s)\n", to_string(r.reason)); std::abort(); }
      if (r.status == OrderStatus::Accepted || r.status == OrderStatus::PartiallyFilled) { alive_[op.id] = 1; live_.push_back(op.id); }
      for (const Trade& t : trades_) {
        if (!shadow_.book(op.symbol).find_order(t.maker_id, tmp_)) alive_[t.maker_id] = 0;
      }
      fold_trades(expected_, trades_);
    } else {
      auto c = shadow_.cancel(op.symbol, op.id);
      if (!c.ok) { std::fprintf(stderr, "generator produced failing cancel\n"); std::abort(); }
      alive_[op.id] = 0;
    }
  }

  void finish(Stream& s) {
    expected_.open_orders = shadow_.open_orders();
    s.expected = expected_;
  }

  const Options& o_;
  std::mt19937_64 rng_;
  Engine shadow_;
  std::vector<Price> mid_;
  std::vector<Trade> trades_;
  std::vector<OrderId> live_;
  std::vector<uint8_t> alive_;
  std::vector<SymbolId> id_symbol_;
  OrderId last_id_ = 0;
  Expected expected_;
  Order tmp_{};
};

// --------------------------------------------------------------- results ----
struct TrialResult {
  double seconds = 0;
  double ops_per_sec = 0;
  double cpu_util = 0;
  uint64_t allocs = 0;
  uint64_t rss_kb = 0;
  // latency in ns (only with --latency)
  double mean = 0, p50 = 0, p90 = 0, p95 = 0, p99 = 0, p999 = 0, p9999 = 0, max = 0;
  bool valid = true;
  std::string why;
};

static uint64_t read_vm_hwm_kb() {
  std::ifstream f("/proc/self/status");
  std::string k;
  uint64_t v = 0;
  while (f >> k) {
    if (k == "VmHWM:") { f >> v; break; }
    std::string rest; std::getline(f, rest);
  }
  return v;
}

static double cpu_seconds() {
  rusage ru{};
  getrusage(RUSAGE_THREAD, &ru);
  return ru.ru_utime.tv_sec + ru.ru_utime.tv_usec * 1e-6 + ru.ru_stime.tv_sec + ru.ru_stime.tv_usec * 1e-6;
}

static void pin_to(int cpu) {
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  sched_setaffinity(0, sizeof set, &set);
}

static void percentiles(std::vector<uint32_t>& lat, TrialResult& r) {
  if (lat.empty()) return;
  auto pct = [&](double p) {
    size_t k = static_cast<size_t>(p * static_cast<double>(lat.size() - 1));
    std::nth_element(lat.begin(), lat.begin() + static_cast<std::ptrdiff_t>(k), lat.end());
    return static_cast<double>(lat[k]);
  };
  double sum = 0;
  for (uint32_t v : lat) sum += v;
  r.mean = sum / static_cast<double>(lat.size());
  r.p50 = pct(0.50); r.p90 = pct(0.90); r.p95 = pct(0.95); r.p99 = pct(0.99);
  r.p999 = pct(0.999); r.p9999 = pct(0.9999);
  r.max = static_cast<double>(*std::max_element(lat.begin(), lat.end()));
}

// ------------------------------------------------------------- execution ----
template <class Book>
struct Runner {
  using Eng = EngineT<Book>;

  static void apply_all(Eng& eng, const std::vector<Op>& ops, std::vector<Trade>& trades, Expected& acc) {
    for (const Op& op : ops) {
      trades.clear();
      if (op.kind == 0) eng.submit(op.symbol, NewOrder{op.id, op.price, op.qty, op.side, op.type}, trades);
      else eng.cancel(op.symbol, op.id);
      fold_trades(acc, trades);
    }
  }

  // One timed trial on a fresh (cleared) engine.
  static TrialResult trial(Eng& eng, const Stream& s, bool latency, std::vector<uint32_t>& lat) {
    for (SymbolId i = 0; i < eng.symbol_count(); ++i) eng.book(i).clear();
    std::vector<Trade> trades;
    trades.reserve(1024);
    Expected acc;
    apply_all(eng, s.prefill, trades, acc);

    TrialResult r;
    g_allocs.store(0);
    t_count_allocs = true;
    double cpu0 = cpu_seconds();
    auto t0 = Clock::now();
    if (latency) {
      lat.resize(s.ops.size());
      size_t i = 0;
      auto prev = Clock::now();
      for (const Op& op : s.ops) {
        trades.clear();
        if (op.kind == 0) eng.submit(op.symbol, NewOrder{op.id, op.price, op.qty, op.side, op.type}, trades);
        else eng.cancel(op.symbol, op.id);
        auto now = Clock::now();
        lat[i++] = static_cast<uint32_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now - prev).count());
        prev = now;
        fold_trades(acc, trades);
      }
    } else {
      apply_all(eng, s.ops, trades, acc);
    }
    auto t1 = Clock::now();
    double cpu1 = cpu_seconds();
    t_count_allocs = false;
    r.allocs = g_allocs.load();
    r.seconds = std::chrono::duration<double>(t1 - t0).count();
    r.ops_per_sec = static_cast<double>(s.ops.size()) / r.seconds;
    r.cpu_util = (cpu1 - cpu0) / r.seconds;
    r.rss_kb = read_vm_hwm_kb();
    if (latency) percentiles(lat, r);

    // Validate: identical executions and final state as the shadow run.
    acc.open_orders = eng.open_orders();
    if (acc.trades != s.expected.trades || acc.checksum != s.expected.checksum || acc.open_orders != s.expected.open_orders) {
      r.valid = false;
      std::ostringstream w;
      w << "trades " << acc.trades << "/" << s.expected.trades << " checksum " << acc.checksum << "/"
        << s.expected.checksum << " open " << acc.open_orders << "/" << s.expected.open_orders;
      r.why = w.str();
    }
    for (SymbolId i = 0; i < eng.symbol_count(); ++i) {
      std::string why;
      if (!eng.book(i).check_invariants(&why)) { r.valid = false; r.why += " invariants: " + why; }
    }
    return r;
  }
};

struct Summary {
  std::string workload, book, description;
  size_t ops = 0;
  int threads = 1, symbols = 1, trials = 0;
  double median_ops = 0, min_ops = 0, max_ops = 0, cpu_util = 0, allocs_per_op = 0;
  uint64_t rss_kb = 0, trades = 0;
  TrialResult lat;  // latency from the median trial
  bool valid = true;
  std::string why;
};

static Summary summarize(const std::vector<TrialResult>& trials) {
  Summary s;
  std::vector<double> v;
  for (auto& t : trials) { v.push_back(t.ops_per_sec); if (!t.valid) { s.valid = false; s.why = t.why; } }
  std::vector<double> sorted = v;
  std::sort(sorted.begin(), sorted.end());
  s.median_ops = sorted[sorted.size() / 2];
  s.min_ops = sorted.front();
  s.max_ops = sorted.back();
  size_t mi = 0;
  for (size_t i = 0; i < v.size(); ++i) if (v[i] == s.median_ops) mi = i;
  s.lat = trials[mi];
  s.cpu_util = trials[mi].cpu_util;
  s.rss_kb = trials[mi].rss_kb;
  s.trials = static_cast<int>(trials.size());
  return s;
}

static void print(const Summary& s, const Options& o) {
  std::printf("%-14s %-10s thr=%d sym=%d ops=%zu trials=%d  median %.3f M ops/s (min %.3f, max %.3f)  cpu %.0f%%  allocs/op %.4f  peakRSS %.1f MB  trades %lu %s\n",
              s.workload.c_str(), s.book.c_str(), s.threads, s.symbols, s.ops, s.trials, s.median_ops / 1e6,
              s.min_ops / 1e6, s.max_ops / 1e6, s.cpu_util * 100, s.allocs_per_op, s.rss_kb / 1024.0,
              static_cast<unsigned long>(s.trades), s.valid ? "VALID" : ("INVALID: " + s.why).c_str());
  if (o.latency && s.lat.max > 0)
    std::printf("    latency ns: mean %.0f  p50 %.0f  p90 %.0f  p95 %.0f  p99 %.0f  p99.9 %.0f  p99.99 %.0f  max %.0f\n",
                s.lat.mean, s.lat.p50, s.lat.p90, s.lat.p95, s.lat.p99, s.lat.p999, s.lat.p9999, s.lat.max);
  std::fflush(stdout);
}

static void append_json(const Summary& s, const Options& o) {
  if (o.json.empty()) return;
  std::ofstream f(o.json, std::ios::app);
  f << "{\"workload\":\"" << s.workload << "\",\"book\":\"" << s.book << "\",\"threads\":" << s.threads
    << ",\"symbols\":" << s.symbols << ",\"ops\":" << s.ops << ",\"trials\":" << s.trials
    << ",\"median_ops_per_sec\":" << static_cast<uint64_t>(s.median_ops) << ",\"min_ops_per_sec\":" << static_cast<uint64_t>(s.min_ops)
    << ",\"max_ops_per_sec\":" << static_cast<uint64_t>(s.max_ops) << ",\"cpu_util\":" << s.cpu_util
    << ",\"allocs_per_op\":" << s.allocs_per_op << ",\"peak_rss_kb\":" << s.rss_kb << ",\"trades\":" << s.trades
    << ",\"latency\":" << (o.latency ? "true" : "false") << ",\"mean_ns\":" << s.lat.mean << ",\"p50_ns\":" << s.lat.p50
    << ",\"p90_ns\":" << s.lat.p90 << ",\"p95_ns\":" << s.lat.p95 << ",\"p99_ns\":" << s.lat.p99 << ",\"p999_ns\":" << s.lat.p999
    << ",\"p9999_ns\":" << s.lat.p9999 << ",\"max_ns\":" << s.lat.max << ",\"valid\":" << (s.valid ? "true" : "false")
    << ",\"pinned\":" << (o.pin_set ? "true" : "false") << ",\"description\":\"" << s.description << "\"}\n";
}

template <class Book>
static Summary run_single(const Options& o, const std::string& name, const Stream& s, int symbols) {
  EngineT<Book> eng(book_config(o));
  for (int i = 0; i < symbols; ++i) eng.add_symbol("S" + std::to_string(i));
  std::vector<uint32_t> lat;
  std::vector<TrialResult> trials;
  // Trial 0 is a warm-up (page faults, hash growth, code cache) and discarded.
  for (int t = 0; t <= o.trials; ++t) {
    TrialResult r = Runner<Book>::trial(eng, s, o.latency, lat);
    if (t > 0) trials.push_back(r);
    if (!r.valid) std::fprintf(stderr, "trial %d INVALID: %s\n", t, r.why.c_str());
  }
  Summary sum = summarize(trials);
  sum.workload = name; sum.book = o.book; sum.description = s.description; sum.ops = s.ops.size();
  sum.symbols = symbols; sum.trades = s.expected.trades;
  sum.allocs_per_op = static_cast<double>(sum.lat.allocs) / static_cast<double>(s.ops.size());
  return sum;
}

// Independent shards: T threads, each its own engine + op stream, started
// together; aggregate throughput = total ops / slowest thread.
template <class Book>
static Summary run_shards(const Options& o, const std::vector<Stream>& streams) {
  int T = static_cast<int>(streams.size());
  std::vector<std::unique_ptr<EngineT<Book>>> engines;
  for (int i = 0; i < T; ++i) {
    engines.push_back(std::make_unique<EngineT<Book>>(book_config(o)));
    for (int s = 0; s < o.symbols; ++s) engines.back()->add_symbol("S" + std::to_string(s));
  }
  std::vector<TrialResult> trials;
  uint64_t total_trades = 0;
  for (auto& s : streams) total_trades += s.expected.trades;
  for (int t = 0; t <= o.trials; ++t) {
    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    std::vector<TrialResult> per(static_cast<size_t>(T));
    std::vector<std::thread> ths;
    for (int i = 0; i < T; ++i) {
      ths.emplace_back([&, i] {
        if (o.pin_set) pin_to((o.pin + i) % static_cast<int>(std::thread::hardware_concurrency()));
        auto& eng = *engines[static_cast<size_t>(i)];
        const Stream& s = streams[static_cast<size_t>(i)];
        for (SymbolId k = 0; k < eng.symbol_count(); ++k) eng.book(k).clear();
        std::vector<Trade> trades;
        trades.reserve(1024);
        Expected acc;
        Runner<Book>::apply_all(eng, s.prefill, trades, acc);
        ready.fetch_add(1);
        while (!go.load(std::memory_order_acquire)) __builtin_ia32_pause();
        auto t0 = Clock::now();
        Runner<Book>::apply_all(eng, s.ops, trades, acc);
        auto t1 = Clock::now();
        TrialResult& r = per[static_cast<size_t>(i)];
        r.seconds = std::chrono::duration<double>(t1 - t0).count();
        r.ops_per_sec = static_cast<double>(s.ops.size()) / r.seconds;
        acc.open_orders = eng.open_orders();
        if (acc.trades != s.expected.trades || acc.checksum != s.expected.checksum || acc.open_orders != s.expected.open_orders) {
          r.valid = false; r.why = "shard " + std::to_string(i) + " mismatch";
        }
      });
    }
    while (ready.load() < T) std::this_thread::yield();
    auto t0 = Clock::now();
    go.store(true, std::memory_order_release);
    for (auto& th : ths) th.join();
    auto t1 = Clock::now();
    TrialResult agg;
    size_t total_ops = 0;
    double slowest = 0;
    for (int i = 0; i < T; ++i) {
      total_ops += streams[static_cast<size_t>(i)].ops.size();
      slowest = std::max(slowest, per[static_cast<size_t>(i)].seconds);
      if (!per[static_cast<size_t>(i)].valid) { agg.valid = false; agg.why = per[static_cast<size_t>(i)].why; }
    }
    agg.seconds = std::chrono::duration<double>(t1 - t0).count();
    agg.ops_per_sec = static_cast<double>(total_ops) / slowest;
    agg.cpu_util = static_cast<double>(T);
    agg.rss_kb = read_vm_hwm_kb();
    if (t > 0) trials.push_back(agg);
  }
  Summary sum = summarize(trials);
  sum.workload = "shards"; sum.book = o.book; sum.threads = T; sum.symbols = o.symbols; sum.trades = total_trades;
  sum.ops = 0; for (auto& s : streams) sum.ops += s.ops.size();
  sum.description = "independent single-writer shards, each replaying a mixed stream";
  return sum;
}

// Round trip through the MPSC queue to a shard thread and back (the
// in-process cost a gRPC handler pays on top of the book itself).
static Summary run_queue(const Options& o, const Stream& s, bool pipelined) {
  ShardedEngineConfig cfg;
  cfg.book = book_config(o);
  cfg.shards = 1;
  if (o.pin_set) cfg.cpu_affinity = {o.pin + 1};
  ShardedEngine se(cfg);
  SymbolRef ref = se.add_symbol("S0");
  se.start();
  Request base;
  base.symbol = ref.local_id;
  auto to_req = [&](const Op& op, Request& r) {
    r.symbol = ref.local_id;
    r.trades.clear();
    if (op.kind == 0) { r.type = RequestType::Submit; r.order = NewOrder{op.id, op.price, op.qty, op.side, op.type}; }
    else { r.type = RequestType::Cancel; r.cancel_id = op.id; }
  };
  std::vector<TrialResult> trials;
  std::vector<uint32_t> lat;
  const size_t batch = 64;
  std::vector<std::unique_ptr<Request>> reqs;
  for (size_t i = 0; i < batch; ++i) reqs.push_back(std::make_unique<Request>());
  for (int t = 0; t <= o.trials; ++t) {
    // reset book through the shard thread (clear is not a request type; do it directly while idle)
    se.stop();
    se.engine(0).book(0).clear();
    se.start();
    Expected acc;
    for (const Op& op : s.prefill) { to_req(op, *reqs[0]); se.call(0, *reqs[0]); fold_trades(acc, reqs[0]->trades); }
    TrialResult r;
    if (o.latency) lat.resize(s.ops.size());
    auto t0 = Clock::now();
    if (!pipelined) {
      for (size_t i = 0; i < s.ops.size(); ++i) {
        auto a = Clock::now();
        to_req(s.ops[i], *reqs[0]);
        se.call(0, *reqs[0]);
        if (o.latency) lat[i] = static_cast<uint32_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - a).count());
        fold_trades(acc, reqs[0]->trades);
      }
    } else {
      for (size_t i = 0; i < s.ops.size(); i += batch) {
        size_t n = std::min(batch, s.ops.size() - i);
        for (size_t j = 0; j < n; ++j) { to_req(s.ops[i + j], *reqs[j]); se.submit(0, reqs[j].get()); }
        for (size_t j = 0; j < n; ++j) { reqs[j]->completion.wait(); fold_trades(acc, reqs[j]->trades); }
      }
    }
    auto t1 = Clock::now();
    r.seconds = std::chrono::duration<double>(t1 - t0).count();
    r.ops_per_sec = static_cast<double>(s.ops.size()) / r.seconds;
    r.cpu_util = 2.0;
    r.rss_kb = read_vm_hwm_kb();
    if (o.latency) percentiles(lat, r);
    se.stop();
    acc.open_orders = se.engine(0).open_orders();
    if (acc.trades != s.expected.trades || acc.checksum != s.expected.checksum || acc.open_orders != s.expected.open_orders) {
      std::ostringstream w;
      w << "trades " << acc.trades << "/" << s.expected.trades << " checksum " << acc.checksum << "/" << s.expected.checksum
        << " open " << acc.open_orders << "/" << s.expected.open_orders;
      r.valid = false; r.why = w.str();
    }
    se.start();
    if (t > 0) trials.push_back(r);
  }
  se.stop();
  Summary sum = summarize(trials);
  sum.workload = pipelined ? "queue_pipelined" : "queue_sync"; sum.book = "dense"; sum.threads = 2; sum.ops = s.ops.size();
  sum.trades = s.expected.trades;
  sum.description = pipelined ? "client thread -> MPSC queue -> shard thread, 64 in flight" : "client thread -> MPSC queue -> shard thread -> completion, 1 in flight";
  return sum;
}

// ----------------------------------------------------------------- main -----
static void print_env(const Options& o) {
  std::string model = "unknown";
  std::ifstream ci("/proc/cpuinfo");
  std::string line;
  while (std::getline(ci, line)) if (line.rfind("model name", 0) == 0) { model = line.substr(line.find(':') + 2); break; }
  std::ifstream gov("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor");
  std::string g;
  if (!(gov >> g)) g = "unavailable";
  std::printf("# cpu: %s (%u hw threads)  governor: %s  pin: %s\n", model.c_str(), std::thread::hardware_concurrency(), g.c_str(),
              o.pin_set ? std::to_string(o.pin).c_str() : "no");
#ifdef ME_CXX_FLAGS
  std::printf("# compiler: %s  flags: %s\n", __VERSION__, ME_CXX_FLAGS);
#else
  std::printf("# compiler: %s\n", __VERSION__);
#endif
  std::printf("# book=%s ops=%zu trials=%d seed=%lu symbols=%d threads=%d prefill=%zu spread=%ld latency=%d mix=%d/%d/%d\n",
              o.book.c_str(), o.ops, o.trials, static_cast<unsigned long>(o.seed), o.symbols, o.threads, o.prefill,
              static_cast<long>(o.spread), o.latency ? 1 : 0, o.mix_insert, o.mix_cancel, o.mix_aggress);
}

template <class Book>
static int run_all(const Options& o) {
  auto want = [&](const char* w) { return o.workload == "all" || o.workload == w; };
  int rc = 0;
  auto emit = [&](Summary s) { print(s, o); append_json(s, o); if (!s.valid) rc = 1; };

  if (want("insert")) { Generator g(o, o.seed, o.symbols); emit(run_single<Book>(o, "insert", g.insert_stream(o.ops), o.symbols)); }
  if (want("cancel")) { Generator g(o, o.seed, o.symbols); emit(run_single<Book>(o, "cancel", g.cancel_stream(o.ops), o.symbols)); }
  if (want("match")) { Generator g(o, o.seed, o.symbols); emit(run_single<Book>(o, "match", g.match_stream(o.ops), o.symbols)); }
  if (want("partial")) { Generator g(o, o.seed, o.symbols); emit(run_single<Book>(o, "partial", g.partial_stream(o.ops), o.symbols)); }
  if (want("mixed")) { Generator g(o, o.seed, o.symbols); emit(run_single<Book>(o, "mixed", g.mixed_stream(o.ops, o.prefill), o.symbols)); }
  if (want("deep")) {
    Options d = o; d.spread = 5000; d.prefill = std::max<size_t>(o.prefill, 2'000'000);
    Generator g(d, o.seed, o.symbols);
    Summary s = run_single<Book>(d, "deep", g.mixed_stream(o.ops, d.prefill), o.symbols);
    emit(s);
  }
  if (want("multisym")) {
    Options m = o; m.symbols = std::max(o.symbols, 64);
    Generator g(m, o.seed, m.symbols);
    emit(run_single<Book>(m, "multisym", g.mixed_stream(o.ops, o.prefill), m.symbols));
  }
  if (want("shards")) {
    std::vector<Stream> streams;
    for (int i = 0; i < o.threads; ++i) { Generator g(o, o.seed + static_cast<uint64_t>(i), o.symbols); streams.push_back(g.mixed_stream(o.ops, o.prefill)); }
    emit(run_shards<Book>(o, streams));
  }
  if (want("queue_sync") || want("queue_pipelined")) {
    Options q = o; q.ops = std::min<size_t>(o.ops, 1'000'000);
    Generator g(q, o.seed, 1);
    Stream s = g.mixed_stream(q.ops, q.prefill);
    if (want("queue_sync")) emit(run_queue(q, s, false));
    if (want("queue_pipelined")) emit(run_queue(q, s, true));
  }
  return rc;
}

int main(int argc, char** argv) {
  Options o = parse(argc, argv);
  if (o.pin_set) pin_to(o.pin);
  if (!o.quiet) print_env(o);
  if (o.book == "dense") return run_all<OrderBook>(o);
  if (o.book == "map") return run_all<MapOrderBook>(o);
  if (o.book == "map_oa") return run_all<MapOrderBookOA>(o);
  if (o.book == "sortedvec") return run_all<SortedVecOrderBook>(o);
  if (o.book == "dense_std") return run_all<DenseOrderBookStd>(o);
  if (o.book == "reference") return run_all<ReferenceBook>(o);
  std::fprintf(stderr, "unknown --book %s\n", o.book.c_str());
  return 2;
}
