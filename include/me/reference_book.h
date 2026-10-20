// Deliberately simple reference matching engine used as the oracle for
// differential/randomized testing and as the "baseline" in benchmarks.
// Plain STL containers, one heap allocation per order, no tricks.
#pragma once

#include <list>
#include <map>
#include <unordered_map>
#include <vector>

#include "me/types.h"

namespace me {

class ReferenceBook {
 public:
  explicit ReferenceBook(const BookConfig& cfg = BookConfig{}) : cfg_(cfg) {}

  SubmitResult submit(const NewOrder& o, std::vector<Trade>& trades) {
    SubmitResult r;
    r.remaining_qty = o.qty;
    auto reject = [&](RejectReason why) { r.status = OrderStatus::Rejected; r.reason = why; return r; };
    if (o.id == 0) return reject(RejectReason::InvalidOrderId);
    if (o.qty == 0) return reject(RejectReason::InvalidQuantity);
    if (o.side != Side::Buy && o.side != Side::Sell) return reject(RejectReason::InvalidSide);
    if (o.type != OrderType::Limit && o.type != OrderType::Market) return reject(RejectReason::InvalidOrderType);
    if (o.type == OrderType::Limit) {
      if (o.price <= 0) return reject(RejectReason::InvalidPrice);
      if (o.price >= cfg_.max_price) return reject(RejectReason::PriceOutOfRange);
    }
    if (index_.count(o.id)) return reject(RejectReason::DuplicateOrderId);

    Qty remaining = o.qty;
    r.first_trade_seq = next_seq_;
    const bool limit = o.type == OrderType::Limit;
    if (o.side == Side::Buy) {
      while (remaining > 0 && !asks_.empty()) {
        auto it = asks_.begin();
        if (limit && it->first > o.price) break;
        remaining = match_level(o, it->second, remaining, trades, r);
        if (it->second.empty()) asks_.erase(it);
      }
    } else {
      while (remaining > 0 && !bids_.empty()) {
        auto it = bids_.begin();
        if (limit && it->first < o.price) break;
        remaining = match_level(o, it->second, remaining, trades, r);
        if (it->second.empty()) bids_.erase(it);
      }
    }
    r.filled_qty = o.qty - remaining;
    r.remaining_qty = remaining;
    if (remaining == 0) { r.status = OrderStatus::Filled; return r; }
    if (!limit) { r.status = OrderStatus::Canceled; return r; }
    if (index_.size() >= cfg_.max_open_orders) return reject(RejectReason::BookFull);
    RefOrder ro{o.id, o.price, remaining, o.side};
    if (o.side == Side::Buy) bids_[o.price].push_back(ro); else asks_[o.price].push_back(ro);
    index_[o.id] = {o.price, o.side};
    r.status = r.filled_qty == 0 ? OrderStatus::Accepted : OrderStatus::PartiallyFilled;
    return r;
  }

  CancelResult cancel(OrderId id) {
    CancelResult r;
    auto it = index_.find(id);
    if (it == index_.end()) { r.reason = RejectReason::UnknownOrder; return r; }
    Price p = it->second.first;
    Side s = it->second.second;
    auto erase_from = [&](auto& side) {
      auto lit = side.find(p);
      for (auto oit = lit->second.begin(); oit != lit->second.end(); ++oit) {
        if (oit->id == id) {
          r.canceled_qty = oit->remaining;
          lit->second.erase(oit);
          break;
        }
      }
      if (lit->second.empty()) side.erase(lit);
    };
    if (s == Side::Buy) erase_from(bids_); else erase_from(asks_);
    index_.erase(it);
    r.ok = true;
    r.price = p;
    r.side = s;
    return r;
  }

  BestBidAsk best_bid_ask() const {
    BestBidAsk b;
    if (!bids_.empty()) { b.has_bid = true; b.bid_price = bids_.begin()->first; b.bid_qty = total(bids_.begin()->second); }
    if (!asks_.empty()) { b.has_ask = true; b.ask_price = asks_.begin()->first; b.ask_qty = total(asks_.begin()->second); }
    return b;
  }

  DepthSnapshot snapshot(size_t max_levels) const {
    DepthSnapshot d;
    size_t n = 0;
    for (auto it = bids_.begin(); it != bids_.end() && n < max_levels; ++it, ++n)
      d.bids.push_back({it->first, total(it->second), static_cast<uint32_t>(it->second.size())});
    n = 0;
    for (auto it = asks_.begin(); it != asks_.end() && n < max_levels; ++it, ++n)
      d.asks.push_back({it->first, total(it->second), static_cast<uint32_t>(it->second.size())});
    return d;
  }

  void clear() { bids_.clear(); asks_.clear(); index_.clear(); next_seq_ = 1; }
  size_t open_orders() const { return index_.size(); }
  SeqNo last_seq() const { return next_seq_ - 1; }
  bool check_invariants(std::string* = nullptr) const { return true; }

 private:
  struct RefOrder { OrderId id; Price price; Qty remaining; Side side; };
  using Queue = std::list<RefOrder>;

  static TotalQty total(const Queue& q) {
    TotalQty t = 0;
    for (auto& o : q) t += o.remaining;
    return t;
  }

  Qty match_level(const NewOrder& o, Queue& q, Qty remaining, std::vector<Trade>& trades, SubmitResult& r) {
    while (remaining > 0 && !q.empty()) {
      RefOrder& m = q.front();
      Qty fill = std::min(remaining, m.remaining);
      trades.push_back(Trade{next_seq_++, o.id, m.id, m.price, fill, o.side});
      ++r.trade_count;
      remaining -= fill;
      m.remaining -= fill;
      if (m.remaining == 0) { index_.erase(m.id); q.pop_front(); }
    }
    return remaining;
  }

  BookConfig cfg_;
  std::map<Price, Queue, std::greater<Price>> bids_;
  std::map<Price, Queue> asks_;
  std::unordered_map<OrderId, std::pair<Price, Side>> index_;
  SeqNo next_seq_ = 1;
};

}  // namespace me
