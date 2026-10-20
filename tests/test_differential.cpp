// Differential testing: random operation sequences are replayed against the
// optimized book and the ReferenceBook; every result, every trade and every
// snapshot must be identical.
#include <gtest/gtest.h>

#include <random>
#include <sstream>

#include "me/order_book.h"
#include "me/reference_book.h"

using namespace me;

namespace {

struct Scenario {
  uint64_t seed;
  int ops;
  Price mid;
  Price spread;     // price band half-width around mid
  int market_pct;   // percentage of market orders among submits
  int cancel_pct;   // percentage of cancels among all ops
  Qty max_qty;
};

std::string describe(const Trade& t) {
  std::ostringstream os;
  os << "seq=" << t.seq << " taker=" << t.taker_id << " maker=" << t.maker_id << " px=" << t.price
     << " qty=" << t.qty << " side=" << to_string(t.taker_side);
  return os.str();
}

template <class Book>
void run_scenario(const Scenario& sc) {
  BookConfig cfg{1 << 16, 1 << 16};
  Book fast(cfg);
  ReferenceBook ref(cfg);
  std::mt19937_64 rng(sc.seed);
  std::vector<Trade> tf, tr;
  std::vector<OrderId> live;
  OrderId next_id = 1;
  size_t total_trades = 0;

  for (int i = 0; i < sc.ops; ++i) {
    tf.clear();
    tr.clear();
    int roll = static_cast<int>(rng() % 100);
    if (roll < sc.cancel_pct && !live.empty()) {
      size_t k = rng() % live.size();
      OrderId id = live[k];
      // Occasionally cancel an id that may already be gone (or never existed).
      if (rng() % 10 == 0) id = rng() % (next_id + 5);
      auto a = fast.cancel(id);
      auto b = ref.cancel(id);
      ASSERT_EQ(a.ok, b.ok) << "op " << i << " cancel " << id;
      ASSERT_EQ(a.reason, b.reason);
      ASSERT_EQ(a.canceled_qty, b.canceled_qty);
      ASSERT_EQ(a.price, b.price);
      ASSERT_EQ(a.side, b.side);
      if (a.ok) {
        for (size_t j = 0; j < live.size(); ++j)
          if (live[j] == id) { live[j] = live.back(); live.pop_back(); break; }
      }
    } else {
      NewOrder o;
      // Mostly fresh ids, sometimes a deliberate duplicate / invalid order.
      int bad = static_cast<int>(rng() % 50);
      o.id = bad == 0 && !live.empty() ? live[rng() % live.size()] : next_id++;
      o.side = (rng() & 1) ? Side::Buy : Side::Sell;
      o.type = static_cast<int>(rng() % 100) < sc.market_pct ? OrderType::Market : OrderType::Limit;
      o.qty = static_cast<Qty>(rng() % (sc.max_qty + 1));  // 0 sometimes -> rejected
      if (bad == 1) o.qty = 0;
      o.price = sc.mid + static_cast<Price>(rng() % (2 * sc.spread + 1)) - sc.spread;
      if (bad == 2) o.price = 0;
      if (bad == 3) o.price = 1 << 16;
      auto a = fast.submit(o, tf);
      auto b = ref.submit(o, tr);
      ASSERT_EQ(a.status, b.status) << "op " << i << " id " << o.id;
      ASSERT_EQ(a.reason, b.reason) << "op " << i;
      ASSERT_EQ(a.filled_qty, b.filled_qty) << "op " << i;
      ASSERT_EQ(a.remaining_qty, b.remaining_qty) << "op " << i;
      ASSERT_EQ(a.trade_count, b.trade_count) << "op " << i;
      ASSERT_EQ(tf.size(), tr.size()) << "op " << i;
      for (size_t t = 0; t < tf.size(); ++t) {
        ASSERT_EQ(describe(tf[t]), describe(tr[t])) << "op " << i << " trade " << t;
      }
      total_trades += tf.size();
      if (a.status == OrderStatus::Accepted || a.status == OrderStatus::PartiallyFilled) live.push_back(o.id);
    }
    ASSERT_EQ(fast.open_orders(), ref.open_orders()) << "op " << i;
    if (i % 257 == 0) {
      auto bf = fast.best_bid_ask();
      auto br = ref.best_bid_ask();
      ASSERT_EQ(bf.has_bid, br.has_bid);
      ASSERT_EQ(bf.has_ask, br.has_ask);
      ASSERT_EQ(bf.bid_price, br.bid_price);
      ASSERT_EQ(bf.ask_price, br.ask_price);
      ASSERT_EQ(bf.bid_qty, br.bid_qty);
      ASSERT_EQ(bf.ask_qty, br.ask_qty);
      auto df = fast.snapshot(10);
      auto dr = ref.snapshot(10);
      ASSERT_EQ(df.bids.size(), dr.bids.size());
      ASSERT_EQ(df.asks.size(), dr.asks.size());
      for (size_t k = 0; k < df.bids.size(); ++k) {
        ASSERT_EQ(df.bids[k].price, dr.bids[k].price);
        ASSERT_EQ(df.bids[k].qty, dr.bids[k].qty);
        ASSERT_EQ(df.bids[k].order_count, dr.bids[k].order_count);
      }
      for (size_t k = 0; k < df.asks.size(); ++k) {
        ASSERT_EQ(df.asks[k].price, dr.asks[k].price);
        ASSERT_EQ(df.asks[k].qty, dr.asks[k].qty);
        ASSERT_EQ(df.asks[k].order_count, dr.asks[k].order_count);
      }
      std::string why;
      ASSERT_TRUE(fast.check_invariants(&why)) << why;
    }
  }
  ASSERT_EQ(fast.last_seq(), ref.last_seq());
  EXPECT_GT(total_trades, 0u);
}

const Scenario kScenarios[] = {
    {1, 100000, 1000, 10, 5, 30, 20},     // tight band, lots of crossing
    {2, 100000, 1000, 200, 2, 40, 100},   // wide band, deep book
    {3, 100000, 50000, 3, 20, 10, 5},     // very tight, market heavy
    {4, 100000, 300, 299, 0, 50, 1000},   // prices down to 1, heavy cancel
    {5, 50000, 60000, 5535, 10, 20, 50},  // touches the top of the price band
    {6, 200000, 2000, 50, 5, 33, 10},
};

}  // namespace

TEST(Differential, DenseBookMatchesReference) {
  for (const auto& sc : kScenarios) {
    SCOPED_TRACE("seed " + std::to_string(sc.seed));
    run_scenario<OrderBook>(sc);
  }
}

TEST(Differential, MapBookMatchesReference) {
  for (const auto& sc : kScenarios) {
    SCOPED_TRACE("seed " + std::to_string(sc.seed));
    run_scenario<MapOrderBook>(sc);
  }
}

TEST(Differential, SortedVecBookMatchesReference) {
  for (const auto& sc : kScenarios) {
    SCOPED_TRACE("seed " + std::to_string(sc.seed));
    run_scenario<SortedVecOrderBook>(sc);
  }
}

TEST(Differential, ManySeedsShort) {
  for (uint64_t seed = 100; seed < 160; ++seed) {
    SCOPED_TRACE("seed " + std::to_string(seed));
    Scenario sc{seed, 5000, static_cast<Price>(100 + seed), static_cast<Price>(1 + seed % 30),
                static_cast<int>(seed % 25), static_cast<int>(seed % 50), static_cast<Qty>(1 + seed % 40)};
    run_scenario<OrderBook>(sc);
  }
}
