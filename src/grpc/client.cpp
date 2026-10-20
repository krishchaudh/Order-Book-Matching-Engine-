// Example command-line client.
//
//   me_client [--target=HOST:PORT] submit  SYMBOL ID buy|sell limit|market PRICE QTY
//   me_client [--target=HOST:PORT] cancel  SYMBOL ID
//   me_client [--target=HOST:PORT] book    SYMBOL [DEPTH]
//   me_client [--target=HOST:PORT] bba     SYMBOL
//   me_client [--target=HOST:PORT] symbols
//   me_client [--target=HOST:PORT] demo    SYMBOL      # scripted crossing scenario
//   me_client [--target=HOST:PORT] stream  SYMBOL N    # N orders over one bidi stream
#include <grpcpp/grpcpp.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "matching_engine.grpc.pb.h"

namespace mev1 = me::v1;

static const char* status_name(mev1::OrderStatus s) { return mev1::OrderStatus_Name(s).c_str(); }

static void print_report(const mev1::ExecutionReport& r) {
  std::printf("%s order %lu: %s", r.symbol().c_str(), static_cast<unsigned long>(r.order_id()), mev1::OrderStatus_Name(r.status()).c_str());
  if (r.status() == mev1::REJECTED) std::printf(" (%s)", mev1::RejectReason_Name(r.reject_reason()).c_str());
  std::printf(" filled=%u remaining=%u\n", r.filled_quantity(), r.remaining_quantity());
  for (auto& f : r.fills())
    std::printf("  fill seq=%lu maker=%lu taker=%lu px=%ld qty=%u\n", static_cast<unsigned long>(f.sequence()),
                static_cast<unsigned long>(f.maker_order_id()), static_cast<unsigned long>(f.taker_order_id()),
                static_cast<long>(f.price()), f.quantity());
}

static bool check(const grpc::Status& st) {
  if (st.ok()) return true;
  std::fprintf(stderr, "RPC failed: %d %s\n", static_cast<int>(st.error_code()), st.error_message().c_str());
  return false;
}

static mev1::ExecutionReport submit(mev1::MatchingEngine::Stub& stub, const std::string& sym, uint64_t id, mev1::Side side,
                                    mev1::OrderType type, int64_t px, uint32_t qty) {
  mev1::SubmitOrderRequest req;
  req.set_symbol(sym); req.set_order_id(id); req.set_side(side); req.set_type(type); req.set_price(px); req.set_quantity(qty);
  mev1::ExecutionReport rep;
  grpc::ClientContext ctx;
  if (check(stub.SubmitOrder(&ctx, req, &rep))) print_report(rep);
  return rep;
}

static void book(mev1::MatchingEngine::Stub& stub, const std::string& sym, uint32_t depth) {
  mev1::GetOrderBookRequest req; req.set_symbol(sym); req.set_depth(depth);
  mev1::GetOrderBookResponse rep; grpc::ClientContext ctx;
  if (!check(stub.GetOrderBook(&ctx, req, &rep))) return;
  std::printf("%s book (top %u)\n   %12s %10s %6s | %-6s %-10s %-12s\n", sym.c_str(), depth, "bid qty", "bid px", "n", "n", "ask px", "ask qty");
  size_t rows = std::max(rep.bids_size(), rep.asks_size());
  for (size_t i = 0; i < rows; ++i) {
    if (static_cast<int>(i) < rep.bids_size()) std::printf("   %12lu %10ld %6u | ", static_cast<unsigned long>(rep.bids(i).quantity()), static_cast<long>(rep.bids(i).price()), rep.bids(i).order_count());
    else std::printf("   %12s %10s %6s | ", "", "", "");
    if (static_cast<int>(i) < rep.asks_size()) std::printf("%-6u %-10ld %-12lu", rep.asks(i).order_count(), static_cast<long>(rep.asks(i).price()), static_cast<unsigned long>(rep.asks(i).quantity()));
    std::printf("\n");
  }
}

int main(int argc, char** argv) {
  std::string target = "localhost:50051";
  std::vector<std::string> args;
  for (int i = 1; i < argc; ++i) {
    if (std::strncmp(argv[i], "--target=", 9) == 0) target = argv[i] + 9;
    else args.emplace_back(argv[i]);
  }
  if (args.empty()) { std::fprintf(stderr, "see header of src/grpc/client.cpp for usage\n"); return 2; }
  auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
  mev1::MatchingEngine::Stub stub(channel);
  const std::string& cmd = args[0];

  if (cmd == "submit" && args.size() == 7) {
    mev1::Side side = args[3] == "buy" ? mev1::BUY : args[3] == "sell" ? mev1::SELL : mev1::SIDE_UNSPECIFIED;
    mev1::OrderType type = args[4] == "limit" ? mev1::LIMIT : args[4] == "market" ? mev1::MARKET : mev1::ORDER_TYPE_UNSPECIFIED;
    submit(stub, args[1], std::strtoull(args[2].c_str(), nullptr, 10), side, type, std::strtoll(args[5].c_str(), nullptr, 10),
           static_cast<uint32_t>(std::strtoul(args[6].c_str(), nullptr, 10)));
  } else if (cmd == "cancel" && args.size() == 3) {
    mev1::CancelOrderRequest req; req.set_symbol(args[1]); req.set_order_id(std::strtoull(args[2].c_str(), nullptr, 10));
    mev1::CancelOrderResponse rep; grpc::ClientContext ctx;
    if (check(stub.CancelOrder(&ctx, req, &rep)))
      std::printf("%s cancel %lu: %s%s qty=%u\n", rep.symbol().c_str(), static_cast<unsigned long>(rep.order_id()), rep.canceled() ? "CANCELED" : "FAILED ",
                  rep.canceled() ? "" : mev1::RejectReason_Name(rep.reject_reason()).c_str(), rep.canceled_quantity());
  } else if (cmd == "book" && args.size() >= 2) {
    book(stub, args[1], args.size() > 2 ? static_cast<uint32_t>(std::atoi(args[2].c_str())) : 10);
  } else if (cmd == "bba" && args.size() == 2) {
    mev1::GetBestBidAskRequest req; req.set_symbol(args[1]);
    mev1::GetBestBidAskResponse rep; grpc::ClientContext ctx;
    if (check(stub.GetBestBidAsk(&ctx, req, &rep)))
      std::printf("%s bid %s%ld x %lu  ask %s%ld x %lu\n", rep.symbol().c_str(), rep.has_bid() ? "" : "(none) ", static_cast<long>(rep.bid_price()),
                  static_cast<unsigned long>(rep.bid_quantity()), rep.has_ask() ? "" : "(none) ", static_cast<long>(rep.ask_price()),
                  static_cast<unsigned long>(rep.ask_quantity()));
  } else if (cmd == "symbols") {
    mev1::ListSymbolsRequest req; mev1::ListSymbolsResponse rep; grpc::ClientContext ctx;
    if (check(stub.ListSymbols(&ctx, req, &rep))) {
      std::printf("%u partitions\n", rep.partitions());
      for (auto& s : rep.symbols()) std::printf("  %-8s partition %u\n", s.symbol().c_str(), s.partition());
    }
  } else if (cmd == "demo" && args.size() == 2) {
    const std::string& sym = args[1];
    uint64_t base = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count()) & 0xFFFFFFFFull;
    std::printf("-- resting liquidity\n");
    submit(stub, sym, base + 1, mev1::BUY, mev1::LIMIT, 1000, 10);
    submit(stub, sym, base + 2, mev1::BUY, mev1::LIMIT, 999, 20);
    submit(stub, sym, base + 3, mev1::SELL, mev1::LIMIT, 1002, 15);
    submit(stub, sym, base + 4, mev1::SELL, mev1::LIMIT, 1002, 5);
    book(stub, sym, 5);
    std::printf("-- aggressive buy 18 @ 1002 fills FIFO: 15 from %lu then 3 from %lu\n", static_cast<unsigned long>(base + 3), static_cast<unsigned long>(base + 4));
    submit(stub, sym, base + 5, mev1::BUY, mev1::LIMIT, 1002, 18);
    std::printf("-- market sell 25 sweeps 10@1000 then 15@999\n");
    submit(stub, sym, base + 6, mev1::SELL, mev1::MARKET, 0, 25);
    std::printf("-- rejections\n");
    submit(stub, sym, base + 6, mev1::SELL, mev1::LIMIT, 1001, 0);
    submit(stub, sym, base + 2, mev1::BUY, mev1::LIMIT, 998, 1);
    submit(stub, "NOPE", base + 7, mev1::BUY, mev1::LIMIT, 998, 1);
    book(stub, sym, 5);
  } else if (cmd == "stream" && args.size() == 3) {
    grpc::ClientContext ctx;
    auto stream = stub.SubmitOrderStream(&ctx);
    int n = std::atoi(args[2].c_str());
    uint64_t base = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count()) & 0xFFFFFFFFull;
    for (int i = 0; i < n; ++i) {
      mev1::SubmitOrderRequest req;
      req.set_symbol(args[1]); req.set_order_id(base + static_cast<uint64_t>(i) + 1);
      req.set_side(i % 2 ? mev1::SELL : mev1::BUY); req.set_type(mev1::LIMIT);
      req.set_price(i % 2 ? 1005 + i % 7 : 995 - i % 7); req.set_quantity(1 + static_cast<uint32_t>(i % 9));
      stream->Write(req);
    }
    stream->WritesDone();
    mev1::ExecutionReport rep;
    int got = 0;
    while (stream->Read(&rep)) { if (got < 5) print_report(rep); ++got; }
    check(stream->Finish());
    std::printf("%d reports received\n", got);
  } else {
    std::fprintf(stderr, "bad command; see header of src/grpc/client.cpp\n");
    return 2;
  }
  return 0;
}
