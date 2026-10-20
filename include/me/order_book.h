// Single-symbol central limit order book with price-time (FIFO) priority.
//
// Hot-path design:
//   * Orders live in a slab (OrderPool) addressed by 32-bit slot index.
//   * Each price level is an intrusive doubly linked FIFO of slot indexes,
//     so cancel is O(1) once the order is located.
//   * OrderId -> slot lookup goes through OrderIndexT (open addressing).
//   * Price levels are discovered through LevelIndexT (dense array + bitmap).
//   * No heap allocation per operation in steady state; trades are appended
//     to a caller-owned vector that keeps its capacity across calls.
//
// Trade price rule: a trade always executes at the resting (maker) order's
// price. Market orders never rest; any unfilled remainder is canceled.
#pragma once

#include <cassert>
#include <cstdint>
#include <vector>

#include "me/level_index.h"
#include "me/order_index.h"
#include "me/order_pool.h"
#include "me/types.h"

namespace me {

#if defined(__GNUC__)
#define ME_LIKELY(x) __builtin_expect(!!(x), 1)
#define ME_UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#define ME_LIKELY(x) (x)
#define ME_UNLIKELY(x) (x)
#endif

template <template <bool> class LevelIndexT, class OrderIndexT>
class OrderBookT {
 public:
  explicit OrderBookT(const BookConfig& cfg = BookConfig{})
      : cfg_(cfg),
        pool_(cfg.max_open_orders),
        // Starts small and doubles with the live-order count: measured faster
        // than pre-sizing to max_open_orders, which spreads a small live set
        // over a huge, cache-cold table (mixed workload -35%).
        index_(1024),
        bids_(cfg.max_price),
        asks_(cfg.max_price) {}

  OrderBookT(const OrderBookT&) = delete;
  OrderBookT& operator=(const OrderBookT&) = delete;

  // Submits an order. Trades (if any) are appended to `trades`.
  SubmitResult submit(const NewOrder& o, std::vector<Trade>& trades) {
    SubmitResult r;
    r.remaining_qty = o.qty;
    if (ME_UNLIKELY(o.id == 0)) return reject(r, RejectReason::InvalidOrderId);
    if (ME_UNLIKELY(o.qty == 0)) return reject(r, RejectReason::InvalidQuantity);
    if (ME_UNLIKELY(o.side != Side::Buy && o.side != Side::Sell)) return reject(r, RejectReason::InvalidSide);
    if (ME_UNLIKELY(o.type != OrderType::Limit && o.type != OrderType::Market))
      return reject(r, RejectReason::InvalidOrderType);
    if (o.type == OrderType::Limit) {
      if (ME_UNLIKELY(o.price <= 0)) return reject(r, RejectReason::InvalidPrice);
      if (ME_UNLIKELY(!bids_.accepts(o.price))) return reject(r, RejectReason::PriceOutOfRange);
    }
    if (ME_UNLIKELY(index_.contains(o.id))) return reject(r, RejectReason::DuplicateOrderId);

    return o.side == Side::Buy ? submit_side<true>(o, trades, r) : submit_side<false>(o, trades, r);
  }

  CancelResult cancel(OrderId id) {
    CancelResult r;
    uint32_t slot = index_.erase(id);
    if (ME_UNLIKELY(slot == kNil)) {
      r.reason = RejectReason::UnknownOrder;
      return r;
    }
    Order& ord = pool_[slot];
    r.ok = true;
    r.canceled_qty = ord.remaining;
    r.price = ord.price;
    r.side = ord.side;
    if (ord.side == Side::Buy) unlink<true>(bids_, slot, ord); else unlink<false>(asks_, slot, ord);
    pool_.release(slot);
    --open_orders_;
    return r;
  }

  BestBidAsk best_bid_ask() {
    BestBidAsk b;
    if (Level* l = bids_.best()) { b.has_bid = true; b.bid_price = l->price; b.bid_qty = l->total_qty; }
    if (Level* l = asks_.best()) { b.has_ask = true; b.ask_price = l->price; b.ask_qty = l->total_qty; }
    return b;
  }

  DepthSnapshot snapshot(size_t max_levels) const {
    DepthSnapshot d;
    bids_.for_each_best_first(max_levels, [&](const Level& l) { d.bids.push_back({l.price, l.total_qty, l.count}); });
    asks_.for_each_best_first(max_levels, [&](const Level& l) { d.asks.push_back({l.price, l.total_qty, l.count}); });
    return d;
  }

  // Lookup of an open order (for tests / API); returns false if absent.
  bool find_order(OrderId id, Order& out) const {
    uint32_t slot = index_.find(id);
    if (slot == kNil) return false;
    out = pool_[slot];
    return true;
  }

  // Removes all orders and resets the sequence counter (end-of-session
  // reset; also used by benchmarks between trials). Keeps reserved memory.
  void clear() {
    pool_.clear();
    index_.clear();
    bids_.clear();
    asks_.clear();
    next_seq_ = 1;
    open_orders_ = 0;
  }

  size_t open_orders() const { return open_orders_; }
  SeqNo last_seq() const { return next_seq_ - 1; }
  size_t bid_levels() const { return bids_.level_count(); }
  size_t ask_levels() const { return asks_.level_count(); }
  const BookConfig& config() const { return cfg_; }

  // Full structural invariant check (slow; tests/benchmarks only).
  bool check_invariants(std::string* why = nullptr) const {
    size_t counted = 0;
    auto check_side = [&](const auto& side, Side s) {
      bool ok = true;
      side.for_each_best_first(static_cast<size_t>(-1), [&](const Level& l) {
        TotalQty sum = 0;
        uint32_t n = 0;
        uint32_t prev = kNil;
        for (uint32_t cur = l.head; cur != kNil; cur = pool_[cur].next) {
          const Order& o = pool_[cur];
          if (o.prev != prev || o.price != l.price || o.side != s || o.remaining == 0 ||
              index_.find(o.id) != cur) {
            ok = false;
          }
          sum += o.remaining;
          ++n;
          prev = cur;
        }
        if (l.tail != prev || sum != l.total_qty || n != l.count || n == 0) ok = false;
        counted += n;
      });
      return ok;
    };
    bool ok = check_side(bids_, Side::Buy) && check_side(asks_, Side::Sell);
    if (!ok && why) *why = "level/order linkage mismatch";
    if (counted != open_orders_ || index_.size() != open_orders_ || pool_.live() != open_orders_) {
      if (why) *why = "order count mismatch";
      return false;
    }
    const Level* bb = const_cast<OrderBookT*>(this)->bids_.best();
    const Level* ba = const_cast<OrderBookT*>(this)->asks_.best();
    if (bb && ba && bb->price >= ba->price) {
      if (why) *why = "crossed book";
      return false;
    }
    return ok;
  }

 private:
  static SubmitResult& reject(SubmitResult& r, RejectReason why) {
    r.status = OrderStatus::Rejected;
    r.reason = why;
    return r;
  }

  template <bool kBid>
  SubmitResult submit_side(const NewOrder& o, std::vector<Trade>& trades, SubmitResult& r) {
    auto& opposite = side_index<!kBid>();
    auto& same = side_index<kBid>();
    const bool is_limit = o.type == OrderType::Limit;
    Qty remaining = o.qty;
    r.first_trade_seq = next_seq_;

    // --- match against the opposite side -------------------------------
    while (remaining > 0) {
      Level* lvl = opposite.best();
      if (lvl == nullptr) break;
      if (is_limit && (kBid ? lvl->price > o.price : lvl->price < o.price)) break;

      while (remaining > 0 && lvl->head != kNil) {
        const uint32_t slot = lvl->head;
        Order& maker = pool_[slot];
        const Qty fill = maker.remaining < remaining ? maker.remaining : remaining;
        trades.push_back(Trade{next_seq_++, o.id, maker.id, lvl->price, fill, o.side});
        ++r.trade_count;
        remaining -= fill;
        maker.remaining -= fill;
        lvl->total_qty -= fill;
        if (maker.remaining == 0) {
          lvl->head = maker.next;
          if (lvl->head == kNil) lvl->tail = kNil; else pool_[lvl->head].prev = kNil;
          --lvl->count;
          index_.erase(maker.id);
          pool_.release(slot);
          --open_orders_;
        }
      }
      if (lvl->head == kNil) opposite.erase(lvl);
    }

    r.filled_qty = o.qty - remaining;
    r.remaining_qty = remaining;
    if (remaining == 0) {
      r.status = OrderStatus::Filled;
      return r;
    }
    if (!is_limit) {
      r.status = OrderStatus::Canceled;
      return r;
    }

    // --- rest the remainder ---------------------------------------------
    const uint32_t slot = pool_.allocate();
    if (ME_UNLIKELY(slot == kNil)) {
      // Fills already happened and are final; the remainder cannot rest.
      r.status = OrderStatus::Rejected;
      r.reason = RejectReason::BookFull;
      return r;
    }
    Order& ord = pool_[slot];
    ord.id = o.id;
    ord.price = o.price;
    ord.remaining = remaining;
    ord.side = o.side;
    ord.next = kNil;
    Level* lvl = same.get_or_create(o.price);
    ord.prev = lvl->tail;
    if (lvl->tail == kNil) lvl->head = slot; else pool_[lvl->tail].next = slot;
    lvl->tail = slot;
    lvl->total_qty += remaining;
    ++lvl->count;
    index_.insert(o.id, slot);
    ++open_orders_;
    r.status = r.filled_qty == 0 ? OrderStatus::Accepted : OrderStatus::PartiallyFilled;
    return r;
  }

  template <bool kBid>
  auto& side_index() {
    if constexpr (kBid) return bids_; else return asks_;
  }

  template <bool kBid, class Idx>
  void unlink(Idx& side, uint32_t /*slot*/, Order& ord) {
    Level* lvl = side.find(ord.price);
    assert(lvl != nullptr);
    if (ord.prev == kNil) lvl->head = ord.next; else pool_[ord.prev].next = ord.next;
    if (ord.next == kNil) lvl->tail = ord.prev; else pool_[ord.next].prev = ord.prev;
    lvl->total_qty -= ord.remaining;
    if (--lvl->count == 0) side.erase(lvl);
  }

  BookConfig cfg_;
  OrderPool pool_;
  OrderIndexT index_;
  LevelIndexT<true> bids_;
  LevelIndexT<false> asks_;
  SeqNo next_seq_ = 1;
  size_t open_orders_ = 0;
};

// The production configuration.
using OrderBook = OrderBookT<DenseLevelIndex, OpenAddressOrderIndex>;

// Alternatives kept for benchmarking competing designs.
using MapOrderBook = OrderBookT<MapLevelIndex, StdOrderIndex>;
using MapOrderBookOA = OrderBookT<MapLevelIndex, OpenAddressOrderIndex>;
using SortedVecOrderBook = OrderBookT<SortedVecLevelIndex, OpenAddressOrderIndex>;
using DenseOrderBookStd = OrderBookT<DenseLevelIndex, StdOrderIndex>;

}  // namespace me
