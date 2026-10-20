// Core value types shared by the matching engine, benchmarks and the gRPC layer.
#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace me {

using Price = int64_t;     // integer price ticks (never floating point)
using Qty = uint32_t;      // per-order quantity
using TotalQty = uint64_t; // aggregated quantity (price level / book totals)
using OrderId = uint64_t;  // client-supplied, unique among open orders of a symbol
using SymbolId = uint32_t;
using SeqNo = uint64_t;    // per-book execution sequence number

constexpr uint32_t kNil = 0xFFFFFFFFu;

enum class Side : uint8_t { Buy = 0, Sell = 1 };
enum class OrderType : uint8_t { Limit = 0, Market = 1 };

enum class OrderStatus : uint8_t {
  Rejected = 0,
  Accepted = 1,         // limit order rested with no fill
  PartiallyFilled = 2,  // limit order partially filled, remainder rested
  Filled = 3,           // fully executed
  Canceled = 4,         // market order remainder canceled (never rests)
};

enum class RejectReason : uint8_t {
  None = 0,
  InvalidOrderId = 1,     // id == 0
  DuplicateOrderId = 2,   // id already open on this book
  InvalidQuantity = 3,    // qty == 0
  InvalidPrice = 4,       // limit price <= 0
  PriceOutOfRange = 5,    // limit price outside the book's configured band
  UnknownSymbol = 6,
  UnknownOrder = 7,       // cancel target not found
  BookFull = 8,           // order pool capacity exhausted
  InvalidSide = 9,
  InvalidOrderType = 10,
  WrongPartition = 11,    // symbol owned by another engine partition
};

struct NewOrder {
  OrderId id = 0;
  Price price = 0;  // ignored for market orders
  Qty qty = 0;
  Side side = Side::Buy;
  OrderType type = OrderType::Limit;
};

struct Trade {
  SeqNo seq;          // monotonic per book, deterministic
  OrderId taker_id;   // incoming (aggressive) order
  OrderId maker_id;   // resting (passive) order
  Price price;        // always the resting order's price
  Qty qty;
  Side taker_side;
};

struct SubmitResult {
  OrderStatus status = OrderStatus::Rejected;
  RejectReason reason = RejectReason::None;
  Qty filled_qty = 0;
  Qty remaining_qty = 0;
  SeqNo first_trade_seq = 0;  // seq of the first trade produced (if any)
  uint32_t trade_count = 0;
};

struct CancelResult {
  bool ok = false;
  RejectReason reason = RejectReason::None;
  Qty canceled_qty = 0;
  Price price = 0;
  Side side = Side::Buy;
};

struct LevelInfo {
  Price price;
  TotalQty qty;
  uint32_t order_count;
};

struct DepthSnapshot {
  std::vector<LevelInfo> bids;  // best (highest) first
  std::vector<LevelInfo> asks;  // best (lowest) first
};

struct BestBidAsk {
  bool has_bid = false;
  bool has_ask = false;
  Price bid_price = 0;
  Price ask_price = 0;
  TotalQty bid_qty = 0;
  TotalQty ask_qty = 0;
};

struct BookConfig {
  // Limit prices must satisfy 0 < price < max_price for dense level indexes.
  Price max_price = 1 << 20;
  // Maximum number of simultaneously open orders. Memory is reserved lazily
  // (untouched pages are not committed) so a generous value is cheap.
  uint32_t max_open_orders = 1u << 22;
};

const char* to_string(Side s);
const char* to_string(OrderType t);
const char* to_string(OrderStatus s);
const char* to_string(RejectReason r);

}  // namespace me
