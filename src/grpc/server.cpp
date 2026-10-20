// gRPC front end for the sharded matching engine.
//
// Threading model: gRPC's synchronous server runs handlers on its own thread
// pool. A handler never touches an order book directly; it builds a Request,
// hands it to the shard thread that exclusively owns the symbol (MPSC queue)
// and blocks on the completion. The shard thread is therefore the single
// writer for every book it owns, matching stays deterministic, and no lock is
// taken on the matching path.
//
// Error model: transport/argument problems are gRPC status codes
// (NOT_FOUND for unknown symbol, INVALID_ARGUMENT for malformed enums,
// UNAVAILABLE while shutting down). Business rejections (duplicate id, zero
// quantity, price out of range, ...) are successful RPCs whose
// ExecutionReport.status == REJECTED with a reject_reason.
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <signal.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "matching_engine.grpc.pb.h"
#include "me/metrics.h"
#include "me/sharded_engine.h"

namespace mev1 = me::v1;
using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

namespace {

struct Options {
  std::string listen = "0.0.0.0:50051";
  uint16_t metrics_port = 9100;
  uint32_t shards = 0;  // 0 = hardware threads / 2, min 1
  std::vector<std::string> symbols = {"AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA", "META", "BRK"};
  std::vector<int> affinity;
  bool measure_latency = true;
  int grpc_threads = 0;  // 0 = library default
  // Partition ownership for Kubernetes: this process owns partitions
  // [partition_index] of [partition_count]; symbols hashing elsewhere are
  // refused with WRONG_PARTITION so two pods never both match one symbol.
  uint32_t partition_index = 0, partition_count = 1;
};

std::vector<std::string> split(const std::string& s, char d) {
  std::vector<std::string> out;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, d)) if (!item.empty()) out.push_back(item);
  return out;
}

Options parse(int argc, char** argv) {
  Options o;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto val = [&](const char* k) -> const char* {
      size_t n = std::strlen(k);
      return a.compare(0, n, k) == 0 && a[n] == '=' ? a.c_str() + n + 1 : nullptr;
    };
    const char* v;
    if ((v = val("--listen"))) o.listen = v;
    else if ((v = val("--metrics-port"))) o.metrics_port = static_cast<uint16_t>(std::atoi(v));
    else if ((v = val("--shards"))) o.shards = static_cast<uint32_t>(std::atoi(v));
    else if ((v = val("--symbols"))) o.symbols = split(v, ',');
    else if ((v = val("--affinity"))) { o.affinity.clear(); for (auto& c : split(v, ',')) o.affinity.push_back(std::atoi(c.c_str())); }
    else if ((v = val("--measure-latency"))) o.measure_latency = std::atoi(v) != 0;
    else if ((v = val("--grpc-threads"))) o.grpc_threads = std::atoi(v);
    else if ((v = val("--partition"))) { unsigned a1 = 0, b1 = 1; std::sscanf(v, "%u/%u", &a1, &b1); o.partition_index = a1; o.partition_count = b1 ? b1 : 1; }
    else { std::fprintf(stderr, "usage: me_server [--listen=HOST:PORT] [--metrics-port=P] [--shards=N] [--symbols=A,B,..] [--affinity=c0,c1,..] [--measure-latency=0|1] [--partition=i/n]\n"); std::exit(2); }
  }
  // Environment overrides (containers / k8s).
  if (const char* e = std::getenv("ME_SYMBOLS")) o.symbols = split(e, ',');
  if (const char* e = std::getenv("ME_SHARDS")) o.shards = static_cast<uint32_t>(std::atoi(e));
  if (const char* e = std::getenv("ME_PARTITION")) { unsigned a1 = 0, b1 = 1; std::sscanf(e, "%u/%u", &a1, &b1); o.partition_index = a1; o.partition_count = b1 ? b1 : 1; }
  if (const char* e = std::getenv("ME_LISTEN")) o.listen = e;
  if (const char* e = std::getenv("ME_METRICS_PORT")) o.metrics_port = static_cast<uint16_t>(std::atoi(e));
  if (o.shards == 0) o.shards = std::max(1u, std::thread::hardware_concurrency() / 2);
  return o;
}

inline mev1::OrderStatus to_pb(me::OrderStatus s) {
  switch (s) {
    case me::OrderStatus::Accepted: return mev1::ACCEPTED;
    case me::OrderStatus::PartiallyFilled: return mev1::PARTIALLY_FILLED;
    case me::OrderStatus::Filled: return mev1::FILLED;
    case me::OrderStatus::Canceled: return mev1::CANCELED;
    case me::OrderStatus::Rejected: return mev1::REJECTED;
  }
  return mev1::ORDER_STATUS_UNSPECIFIED;
}
inline mev1::RejectReason to_pb(me::RejectReason r) { return static_cast<mev1::RejectReason>(static_cast<int>(r)); }

class Service final : public mev1::MatchingEngine::Service {
 public:
  Service(me::ShardedEngine& se, me::metrics::ProcessMetrics& pm, std::vector<std::unique_ptr<me::metrics::ShardMetrics>>& sm,
          std::atomic<bool>& accepting, const Options& o)
      : se_(se), pm_(pm), sm_(sm), accepting_(accepting), opt_(o) {
    // One hook per shard, executed on that shard thread (single writer for
    // its ShardMetrics; plain relaxed increments, no atomics RMW).
    for (uint32_t i = 0; i < se.shard_count(); ++i) {
      me::metrics::ShardMetrics* m = sm_[i].get();
      me::Engine* eng = &se.engine(i);  // only touched from shard i's own thread
      bool timing = o.measure_latency;
      hooks_.emplace_back([m, eng, timing](const me::Request& r) {
        m->operations.inc();
        if (r.type == me::RequestType::Submit) {
          m->orders_received.inc();
          if (r.submit_result.status == me::OrderStatus::Rejected) m->orders_rejected.inc();
          else m->orders_accepted.inc();
          if (!r.trades.empty()) {
            m->trades_executed.inc(r.trades.size());
            uint64_t vol = 0;
            for (auto& t : r.trades) vol += t.qty;
            m->volume_traded.inc(vol);
          }
        } else if (r.type == me::RequestType::Cancel) {
          m->cancels_received.inc();
          if (r.cancel_result.ok) m->orders_canceled.inc();
        }
        m->active_orders.set(static_cast<int64_t>(eng->open_orders()));
        if (timing) m->matching_latency_ns.observe(static_cast<double>(r.engine_ns));
      });
    }
  }

  Status SubmitOrder(ServerContext*, const mev1::SubmitOrderRequest* in, mev1::ExecutionReport* out) override {
    Timer t(*this);
    me::SymbolRef ref;
    Status st = resolve(in->symbol(), ref);
    if (!st.ok()) return t.fail(st);
    me::Request& r = local_request();
    r.type = me::RequestType::Submit;
    r.symbol = ref.local_id;
    r.order = me::NewOrder{in->order_id(), in->price(), in->quantity(), to_side(in->side()), to_type(in->type())};
    r.on_done = &hooks_[ref.shard];
    se_.call(ref.shard, r);
    fill_report(in->symbol(), in->order_id(), r, out);
    observe_queue(r);
    return Status::OK;
  }

  Status CancelOrder(ServerContext*, const mev1::CancelOrderRequest* in, mev1::CancelOrderResponse* out) override {
    Timer t(*this);
    me::SymbolRef ref;
    Status st = resolve(in->symbol(), ref);
    if (!st.ok()) return t.fail(st);
    me::Request& r = local_request();
    r.type = me::RequestType::Cancel;
    r.symbol = ref.local_id;
    r.cancel_id = in->order_id();
    r.on_done = &hooks_[ref.shard];
    se_.call(ref.shard, r);
    out->set_symbol(in->symbol());
    out->set_order_id(in->order_id());
    out->set_canceled(r.cancel_result.ok);
    out->set_reject_reason(to_pb(r.cancel_result.reason));
    out->set_canceled_quantity(r.cancel_result.canceled_qty);
    observe_queue(r);
    return Status::OK;
  }

  Status GetOrderBook(ServerContext*, const mev1::GetOrderBookRequest* in, mev1::GetOrderBookResponse* out) override {
    Timer t(*this);
    me::SymbolRef ref;
    Status st = resolve(in->symbol(), ref);
    if (!st.ok()) return t.fail(st);
    me::Request r;
    r.type = me::RequestType::Snapshot;
    r.symbol = ref.local_id;
    r.depth = in->depth() ? in->depth() : 10;
    se_.call(ref.shard, r);
    out->set_symbol(in->symbol());
    for (auto& l : r.snapshot.bids) { auto* p = out->add_bids(); p->set_price(l.price); p->set_quantity(l.qty); p->set_order_count(l.order_count); }
    for (auto& l : r.snapshot.asks) { auto* p = out->add_asks(); p->set_price(l.price); p->set_quantity(l.qty); p->set_order_count(l.order_count); }
    return Status::OK;
  }

  Status GetBestBidAsk(ServerContext*, const mev1::GetBestBidAskRequest* in, mev1::GetBestBidAskResponse* out) override {
    Timer t(*this);
    me::SymbolRef ref;
    Status st = resolve(in->symbol(), ref);
    if (!st.ok()) return t.fail(st);
    me::Request r;
    r.type = me::RequestType::BestBidAsk;
    r.symbol = ref.local_id;
    se_.call(ref.shard, r);
    out->set_symbol(in->symbol());
    out->set_has_bid(r.bba.has_bid); out->set_bid_price(r.bba.bid_price); out->set_bid_quantity(r.bba.bid_qty);
    out->set_has_ask(r.bba.has_ask); out->set_ask_price(r.bba.ask_price); out->set_ask_quantity(r.bba.ask_qty);
    return Status::OK;
  }

  Status ListSymbols(ServerContext*, const mev1::ListSymbolsRequest*, mev1::ListSymbolsResponse* out) override {
    Timer t(*this);
    for (auto& name : se_.symbols()) {
      me::SymbolRef ref;
      se_.lookup(name, ref);
      auto* s = out->add_symbols();
      s->set_symbol(name);
      s->set_partition(ref.shard);
    }
    out->set_partitions(se_.shard_count());
    return Status::OK;
  }

  Status SubmitOrderStream(ServerContext*, grpc::ServerReaderWriter<mev1::ExecutionReport, mev1::SubmitOrderRequest>* stream) override {
    mev1::SubmitOrderRequest in;
    mev1::ExecutionReport out;
    me::Request& r = local_request();
    while (stream->Read(&in)) {
      auto t0 = std::chrono::steady_clock::now();
      pm_.grpc_requests.add_atomic();
      out.Clear();
      me::SymbolRef ref;
      Status st = resolve(in.symbol(), ref);
      if (!st.ok()) {
        pm_.grpc_errors.add_atomic();
        out.set_symbol(in.symbol()); out.set_order_id(in.order_id()); out.set_status(mev1::REJECTED);
        out.set_reject_reason(st.error_code() == StatusCode::NOT_FOUND ? mev1::UNKNOWN_SYMBOL : mev1::INVALID_ORDER_TYPE);
      } else {
        r.type = me::RequestType::Submit;
        r.symbol = ref.local_id;
        r.order = me::NewOrder{in.order_id(), in.price(), in.quantity(), to_side(in.side()), to_type(in.type())};
        r.on_done = &hooks_[ref.shard];
        se_.call(ref.shard, r);
        fill_report(in.symbol(), in.order_id(), r, &out);
        observe_queue(r);
      }
      if (!stream->Write(out)) break;
      pm_.grpc_latency_ns.observe_atomic(static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count()));
    }
    return Status::OK;
  }

  // Depth gauges rendered on scrape, fetched through the owning shard so the
  // HTTP thread never reads a book concurrently with its writer.
  std::string depth_metrics() {
    if (!accepting_.load()) return {};
    std::ostringstream os;
    os << "# HELP me_book_depth_levels Number of populated price levels per side.\n# TYPE me_book_depth_levels gauge\n";
    std::ostringstream top;
    top << "# HELP me_book_best_price Best bid/ask price in ticks (0 if empty).\n# TYPE me_book_best_price gauge\n";
    for (auto& name : se_.symbols()) {
      me::SymbolRef ref;
      se_.lookup(name, ref);
      me::Request r;
      r.type = me::RequestType::Snapshot;
      r.symbol = ref.local_id;
      r.depth = 1u << 20;
      se_.call(ref.shard, r);
      os << "me_book_depth_levels{symbol=\"" << name << "\",side=\"bid\"} " << r.snapshot.bids.size() << "\n";
      os << "me_book_depth_levels{symbol=\"" << name << "\",side=\"ask\"} " << r.snapshot.asks.size() << "\n";
      top << "me_book_best_price{symbol=\"" << name << "\",side=\"bid\"} " << (r.snapshot.bids.empty() ? 0 : r.snapshot.bids[0].price) << "\n";
      top << "me_book_best_price{symbol=\"" << name << "\",side=\"ask\"} " << (r.snapshot.asks.empty() ? 0 : r.snapshot.asks[0].price) << "\n";
    }
    return os.str() + top.str();
  }

 private:
  struct Timer {
    explicit Timer(Service& s) : s_(s), t0_(std::chrono::steady_clock::now()) { s_.pm_.grpc_requests.add_atomic(); }
    ~Timer() {
      s_.pm_.grpc_latency_ns.observe_atomic(static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0_).count()));
    }
    Status fail(Status st) { s_.pm_.grpc_errors.add_atomic(); return st; }
    Service& s_;
    std::chrono::steady_clock::time_point t0_;
  };

  static me::Side to_side(mev1::Side s) { return s == mev1::BUY ? me::Side::Buy : s == mev1::SELL ? me::Side::Sell : static_cast<me::Side>(0xFF); }
  static me::OrderType to_type(mev1::OrderType t) { return t == mev1::LIMIT ? me::OrderType::Limit : t == mev1::MARKET ? me::OrderType::Market : static_cast<me::OrderType>(0xFF); }

  Status resolve(const std::string& symbol, me::SymbolRef& ref) {
    if (!accepting_.load(std::memory_order_acquire)) return Status(StatusCode::UNAVAILABLE, "engine shutting down");
    if (!se_.lookup(symbol, ref)) {
      if (opt_.partition_count > 1 && me::partition_of(symbol, opt_.partition_count) != opt_.partition_index)
        return Status(StatusCode::FAILED_PRECONDITION, "symbol '" + symbol + "' is owned by partition " +
                                                           std::to_string(me::partition_of(symbol, opt_.partition_count)) + " of " +
                                                           std::to_string(opt_.partition_count) + " (WRONG_PARTITION)");
      return Status(StatusCode::NOT_FOUND, "unknown symbol '" + symbol + "'");
    }
    return Status::OK;
  }

  void fill_report(const std::string& symbol, uint64_t id, const me::Request& r, mev1::ExecutionReport* out) {
    out->set_symbol(symbol);
    out->set_order_id(id);
    out->set_status(to_pb(r.submit_result.status));
    out->set_reject_reason(to_pb(r.submit_result.reason));
    out->set_filled_quantity(r.submit_result.filled_qty);
    out->set_remaining_quantity(r.submit_result.remaining_qty);
    out->set_engine_timestamp_ns(r.done_ts_ns);
    for (auto& t : r.trades) {
      auto* f = out->add_fills();
      f->set_sequence(t.seq); f->set_maker_order_id(t.maker_id); f->set_taker_order_id(t.taker_id);
      f->set_price(t.price); f->set_quantity(t.qty);
    }
  }

  void observe_queue(const me::Request& r) {
    if (opt_.measure_latency) pm_.queue_latency_ns.observe_atomic(static_cast<double>(r.queue_ns));
  }

  static me::Request& local_request() {
    thread_local me::Request r;  // keeps trades capacity; one per handler thread
    r.trades.clear();
    return r;
  }

  me::ShardedEngine& se_;
  me::metrics::ProcessMetrics& pm_;
  std::vector<std::unique_ptr<me::metrics::ShardMetrics>>& sm_;
  std::atomic<bool>& accepting_;
  Options opt_;
  std::vector<std::function<void(const me::Request&)>> hooks_;
};

std::atomic<bool> g_stop{false};
void on_signal(int) { g_stop.store(true); }

}  // namespace

int main(int argc, char** argv) {
  Options o = parse(argc, argv);

  me::ShardedEngineConfig cfg;
  cfg.shards = o.shards;
  cfg.cpu_affinity = o.affinity;
  cfg.measure_latency = o.measure_latency;
  me::ShardedEngine se(cfg);
  uint32_t owned = 0, skipped = 0;
  for (auto& s : o.symbols) {
    if (o.partition_count > 1 && me::partition_of(s, o.partition_count) != o.partition_index) { ++skipped; continue; }
    se.add_symbol(s);
    ++owned;
  }

  std::vector<std::unique_ptr<me::metrics::ShardMetrics>> sm;
  std::vector<me::metrics::ShardMetrics*> smp;
  for (uint32_t i = 0; i < o.shards; ++i) { sm.push_back(std::make_unique<me::metrics::ShardMetrics>()); smp.push_back(sm.back().get()); }
  me::metrics::ProcessMetrics pm;
  pm.symbols.set(owned);
  me::metrics::Registry reg(smp, &pm);
  std::atomic<bool> accepting{false};
  me::metrics::HttpExporter exporter(reg, o.metrics_port, [&] { return accepting.load(); });
  if (!exporter.start()) { std::fprintf(stderr, "cannot bind metrics port %u\n", o.metrics_port); return 1; }

  Service service(se, pm, sm, accepting, o);
  reg.set_extra_provider([&] { return service.depth_metrics(); });
  se.start();

  grpc::EnableDefaultHealthCheckService(true);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(o.listen, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  builder.SetMaxReceiveMessageSize(4 << 20);
  if (o.grpc_threads > 0) {
    builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::MIN_POLLERS, o.grpc_threads);
    builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::MAX_POLLERS, o.grpc_threads);
  }
  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
  if (!server) { std::fprintf(stderr, "failed to start gRPC server on %s\n", o.listen.c_str()); return 1; }
  server->GetHealthCheckService()->SetServingStatus(true);
  accepting.store(true);
  std::printf("me_server listening on %s (metrics :%u) shards=%u symbols=%u owned (%u skipped, partition %u/%u) measure_latency=%d\n",
              o.listen.c_str(), o.metrics_port, o.shards, owned, skipped, o.partition_index, o.partition_count, o.measure_latency ? 1 : 0);
  std::fflush(stdout);

  signal(SIGTERM, on_signal);
  signal(SIGINT, on_signal);
  while (!g_stop.load()) std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Graceful shutdown: stop admitting, fail readiness, drain in-flight RPCs
  // (bounded), then stop shard threads and the exporter.
  std::printf("shutting down\n");
  accepting.store(false);
  server->GetHealthCheckService()->SetServingStatus(false);
  server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(5));
  se.stop();
  exporter.stop();
  return 0;
}
