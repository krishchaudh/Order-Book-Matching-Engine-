// Functional tests for every OrderBookT instantiation plus the reference book.
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "me/order_book.h"
#include "me/reference_book.h"

using namespace me;

template <class Book>
class OrderBookTest : public ::testing::Test {
 protected:
  Book book{BookConfig{1 << 16, 1 << 16}};
  std::vector<Trade> trades;

  SubmitResult limit(OrderId id, Side s, Price p, Qty q) {
    return book.submit(NewOrder{id, p, q, s, OrderType::Limit}, trades);
  }
  SubmitResult market(OrderId id, Side s, Qty q) {
    return book.submit(NewOrder{id, 0, q, s, OrderType::Market}, trades);
  }
  void check() {
    std::string why;
    ASSERT_TRUE(book.check_invariants(&why)) << why;
  }
};

using BookTypes = ::testing::Types<OrderBook, MapOrderBook, MapOrderBookOA, SortedVecOrderBook,
                                   DenseOrderBookStd, ReferenceBook>;
TYPED_TEST_SUITE(OrderBookTest, BookTypes);

TYPED_TEST(OrderBookTest, EmptyBookQueries) {
  auto b = this->book.best_bid_ask();
  EXPECT_FALSE(b.has_bid);
  EXPECT_FALSE(b.has_ask);
  auto d = this->book.snapshot(10);
  EXPECT_TRUE(d.bids.empty());
  EXPECT_TRUE(d.asks.empty());
  EXPECT_EQ(this->book.open_orders(), 0u);
  EXPECT_FALSE(this->book.cancel(42).ok);
  EXPECT_EQ(this->book.cancel(42).reason, RejectReason::UnknownOrder);
}

TYPED_TEST(OrderBookTest, RestingLimitOrdersDoNotCross) {
  auto r1 = this->limit(1, Side::Buy, 100, 10);
  EXPECT_EQ(r1.status, OrderStatus::Accepted);
  EXPECT_EQ(r1.remaining_qty, 10u);
  auto r2 = this->limit(2, Side::Sell, 101, 5);
  EXPECT_EQ(r2.status, OrderStatus::Accepted);
  EXPECT_TRUE(this->trades.empty());
  auto b = this->book.best_bid_ask();
  EXPECT_EQ(b.bid_price, 100);
  EXPECT_EQ(b.bid_qty, 10u);
  EXPECT_EQ(b.ask_price, 101);
  EXPECT_EQ(b.ask_qty, 5u);
  EXPECT_EQ(this->book.open_orders(), 2u);
  this->check();
}

TYPED_TEST(OrderBookTest, CrossingLimitOrderTradesAtRestingPrice) {
  this->limit(1, Side::Sell, 100, 10);
  auto r = this->limit(2, Side::Buy, 105, 10);  // aggressive buy above ask
  EXPECT_EQ(r.status, OrderStatus::Filled);
  EXPECT_EQ(r.filled_qty, 10u);
  ASSERT_EQ(this->trades.size(), 1u);
  EXPECT_EQ(this->trades[0].price, 100);  // maker price
  EXPECT_EQ(this->trades[0].qty, 10u);
  EXPECT_EQ(this->trades[0].maker_id, 1u);
  EXPECT_EQ(this->trades[0].taker_id, 2u);
  EXPECT_EQ(this->trades[0].taker_side, Side::Buy);
  EXPECT_EQ(this->trades[0].seq, 1u);
  EXPECT_EQ(this->book.open_orders(), 0u);
  this->check();
}

TYPED_TEST(OrderBookTest, SellCrossesIntoBids) {
  this->limit(1, Side::Buy, 100, 10);
  this->limit(2, Side::Buy, 99, 10);
  auto r = this->limit(3, Side::Sell, 99, 15);
  EXPECT_EQ(r.status, OrderStatus::Filled);
  ASSERT_EQ(this->trades.size(), 2u);
  EXPECT_EQ(this->trades[0].price, 100);  // best bid first
  EXPECT_EQ(this->trades[0].qty, 10u);
  EXPECT_EQ(this->trades[1].price, 99);
  EXPECT_EQ(this->trades[1].qty, 5u);
  auto b = this->book.best_bid_ask();
  EXPECT_EQ(b.bid_price, 99);
  EXPECT_EQ(b.bid_qty, 5u);
  EXPECT_FALSE(b.has_ask);
  this->check();
}

TYPED_TEST(OrderBookTest, PartialFillRestsRemainder) {
  this->limit(1, Side::Sell, 100, 4);
  auto r = this->limit(2, Side::Buy, 100, 10);
  EXPECT_EQ(r.status, OrderStatus::PartiallyFilled);
  EXPECT_EQ(r.filled_qty, 4u);
  EXPECT_EQ(r.remaining_qty, 6u);
  auto b = this->book.best_bid_ask();
  EXPECT_TRUE(b.has_bid);
  EXPECT_EQ(b.bid_price, 100);
  EXPECT_EQ(b.bid_qty, 6u);
  EXPECT_FALSE(b.has_ask);
  this->check();
}

TYPED_TEST(OrderBookTest, PartialFillOfRestingOrderKeepsPriority) {
  this->limit(1, Side::Sell, 100, 10);
  this->limit(2, Side::Sell, 100, 10);
  auto r = this->limit(3, Side::Buy, 100, 4);
  EXPECT_EQ(r.status, OrderStatus::Filled);
  ASSERT_EQ(this->trades.size(), 1u);
  EXPECT_EQ(this->trades[0].maker_id, 1u);
  // Order 1 keeps 6 at the front of the queue.
  this->trades.clear();
  this->limit(4, Side::Buy, 100, 8);
  ASSERT_EQ(this->trades.size(), 2u);
  EXPECT_EQ(this->trades[0].maker_id, 1u);
  EXPECT_EQ(this->trades[0].qty, 6u);
  EXPECT_EQ(this->trades[1].maker_id, 2u);
  EXPECT_EQ(this->trades[1].qty, 2u);
  this->check();
}

TYPED_TEST(OrderBookTest, FifoPriorityAtSamePrice) {
  for (OrderId id = 1; id <= 5; ++id) this->limit(id, Side::Sell, 100, 1);
  this->limit(100, Side::Buy, 100, 5);
  ASSERT_EQ(this->trades.size(), 5u);
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(this->trades[i].maker_id, i + 1);
    EXPECT_EQ(this->trades[i].seq, i + 1);
  }
  this->check();
}

TYPED_TEST(OrderBookTest, PricePriorityAcrossLevels) {
  this->limit(1, Side::Sell, 103, 1);
  this->limit(2, Side::Sell, 101, 1);
  this->limit(3, Side::Sell, 102, 1);
  this->limit(4, Side::Buy, 110, 3);
  ASSERT_EQ(this->trades.size(), 3u);
  EXPECT_EQ(this->trades[0].price, 101);
  EXPECT_EQ(this->trades[1].price, 102);
  EXPECT_EQ(this->trades[2].price, 103);
  this->check();
}

TYPED_TEST(OrderBookTest, LimitOrderStopsAtItsPrice) {
  this->limit(1, Side::Sell, 101, 5);
  this->limit(2, Side::Sell, 102, 5);
  auto r = this->limit(3, Side::Buy, 101, 8);
  EXPECT_EQ(r.status, OrderStatus::PartiallyFilled);
  EXPECT_EQ(r.filled_qty, 5u);
  auto b = this->book.best_bid_ask();
  EXPECT_EQ(b.bid_price, 101);
  EXPECT_EQ(b.bid_qty, 3u);
  EXPECT_EQ(b.ask_price, 102);
  this->check();
}

TYPED_TEST(OrderBookTest, MarketOrderSweepsAndRemainderIsCanceled) {
  this->limit(1, Side::Sell, 101, 5);
  this->limit(2, Side::Sell, 105, 5);
  auto r = this->market(3, Side::Buy, 12);
  EXPECT_EQ(r.status, OrderStatus::Canceled);
  EXPECT_EQ(r.filled_qty, 10u);
  EXPECT_EQ(r.remaining_qty, 2u);
  ASSERT_EQ(this->trades.size(), 2u);
  EXPECT_EQ(this->trades[0].price, 101);
  EXPECT_EQ(this->trades[1].price, 105);
  auto b = this->book.best_bid_ask();
  EXPECT_FALSE(b.has_bid);  // market orders never rest
  EXPECT_FALSE(b.has_ask);
  EXPECT_EQ(this->book.open_orders(), 0u);
  this->check();
}

TYPED_TEST(OrderBookTest, MarketOrderFullyFilled) {
  this->limit(1, Side::Buy, 99, 10);
  auto r = this->market(2, Side::Sell, 4);
  EXPECT_EQ(r.status, OrderStatus::Filled);
  EXPECT_EQ(r.filled_qty, 4u);
  EXPECT_EQ(this->book.best_bid_ask().bid_qty, 6u);
  this->check();
}

TYPED_TEST(OrderBookTest, MarketOrderOnEmptyBookIsCanceled) {
  auto r = this->market(1, Side::Buy, 5);
  EXPECT_EQ(r.status, OrderStatus::Canceled);
  EXPECT_EQ(r.filled_qty, 0u);
  EXPECT_TRUE(this->trades.empty());
  EXPECT_EQ(this->book.open_orders(), 0u);
}

TYPED_TEST(OrderBookTest, CancelFirstMiddleLast) {
  for (OrderId id = 1; id <= 5; ++id) this->limit(id, Side::Buy, 50, id);
  auto c1 = this->book.cancel(1);  // first
  EXPECT_TRUE(c1.ok);
  EXPECT_EQ(c1.canceled_qty, 1u);
  EXPECT_EQ(c1.price, 50);
  EXPECT_EQ(c1.side, Side::Buy);
  this->check();
  auto c3 = this->book.cancel(3);  // middle
  EXPECT_TRUE(c3.ok);
  this->check();
  auto c5 = this->book.cancel(5);  // last
  EXPECT_TRUE(c5.ok);
  this->check();
  EXPECT_EQ(this->book.best_bid_ask().bid_qty, 2u + 4u);
  // Remaining FIFO is 2 then 4.
  this->limit(10, Side::Sell, 50, 6);
  ASSERT_EQ(this->trades.size(), 2u);
  EXPECT_EQ(this->trades[0].maker_id, 2u);
  EXPECT_EQ(this->trades[1].maker_id, 4u);
  EXPECT_EQ(this->book.open_orders(), 0u);
  this->check();
}

TYPED_TEST(OrderBookTest, CancelRemovesLevelAndUpdatesBest) {
  this->limit(1, Side::Sell, 100, 1);
  this->limit(2, Side::Sell, 101, 1);
  this->limit(3, Side::Sell, 102, 1);
  EXPECT_TRUE(this->book.cancel(1).ok);
  EXPECT_EQ(this->book.best_bid_ask().ask_price, 101);
  EXPECT_TRUE(this->book.cancel(2).ok);
  EXPECT_EQ(this->book.best_bid_ask().ask_price, 102);
  EXPECT_TRUE(this->book.cancel(3).ok);
  EXPECT_FALSE(this->book.best_bid_ask().has_ask);
  EXPECT_FALSE(this->book.cancel(3).ok);  // double cancel
  this->check();
}

TYPED_TEST(OrderBookTest, CancelFilledOrderFails) {
  this->limit(1, Side::Sell, 100, 1);
  this->limit(2, Side::Buy, 100, 1);
  EXPECT_FALSE(this->book.cancel(1).ok);
  EXPECT_FALSE(this->book.cancel(2).ok);
}

TYPED_TEST(OrderBookTest, DuplicateOrderIdRejected) {
  EXPECT_EQ(this->limit(7, Side::Buy, 10, 1).status, OrderStatus::Accepted);
  auto r = this->limit(7, Side::Sell, 11, 1);
  EXPECT_EQ(r.status, OrderStatus::Rejected);
  EXPECT_EQ(r.reason, RejectReason::DuplicateOrderId);
  EXPECT_EQ(this->book.open_orders(), 1u);
  // Once the order is gone the id may be reused.
  EXPECT_TRUE(this->book.cancel(7).ok);
  EXPECT_EQ(this->limit(7, Side::Buy, 10, 1).status, OrderStatus::Accepted);
  this->check();
}

TYPED_TEST(OrderBookTest, InvalidOrdersRejected) {
  EXPECT_EQ(this->limit(0, Side::Buy, 10, 1).reason, RejectReason::InvalidOrderId);
  EXPECT_EQ(this->limit(1, Side::Buy, 10, 0).reason, RejectReason::InvalidQuantity);
  EXPECT_EQ(this->limit(1, Side::Buy, 0, 1).reason, RejectReason::InvalidPrice);
  EXPECT_EQ(this->limit(1, Side::Buy, -5, 1).reason, RejectReason::InvalidPrice);
  EXPECT_EQ(this->limit(1, Side::Buy, 1 << 16, 1).reason, RejectReason::PriceOutOfRange);
  EXPECT_EQ(this->limit(1, Side::Buy, (1 << 16) + 5, 1).reason, RejectReason::PriceOutOfRange);
  NewOrder bad_side{1, 10, 1, static_cast<Side>(7), OrderType::Limit};
  EXPECT_EQ(this->book.submit(bad_side, this->trades).reason, RejectReason::InvalidSide);
  NewOrder bad_type{1, 10, 1, Side::Buy, static_cast<OrderType>(9)};
  EXPECT_EQ(this->book.submit(bad_type, this->trades).reason, RejectReason::InvalidOrderType);
  EXPECT_EQ(this->book.open_orders(), 0u);
  EXPECT_TRUE(this->trades.empty());
  // Highest representable price is accepted.
  EXPECT_EQ(this->limit(2, Side::Sell, (1 << 16) - 1, 1).status, OrderStatus::Accepted);
  this->check();
}

TYPED_TEST(OrderBookTest, DepthSnapshotOrderingAndAggregation) {
  this->limit(1, Side::Buy, 100, 3);
  this->limit(2, Side::Buy, 100, 2);
  this->limit(3, Side::Buy, 98, 7);
  this->limit(4, Side::Buy, 99, 1);
  this->limit(5, Side::Sell, 101, 4);
  this->limit(6, Side::Sell, 103, 9);
  this->limit(7, Side::Sell, 102, 1);
  auto d = this->book.snapshot(2);
  ASSERT_EQ(d.bids.size(), 2u);
  EXPECT_EQ(d.bids[0].price, 100);
  EXPECT_EQ(d.bids[0].qty, 5u);
  EXPECT_EQ(d.bids[0].order_count, 2u);
  EXPECT_EQ(d.bids[1].price, 99);
  ASSERT_EQ(d.asks.size(), 2u);
  EXPECT_EQ(d.asks[0].price, 101);
  EXPECT_EQ(d.asks[1].price, 102);
  auto full = this->book.snapshot(100);
  EXPECT_EQ(full.bids.size(), 3u);
  EXPECT_EQ(full.asks.size(), 3u);
  EXPECT_EQ(full.asks[2].price, 103);
  this->check();
}

TYPED_TEST(OrderBookTest, SequenceNumbersAreMonotonicAndDeterministic) {
  for (OrderId id = 1; id <= 10; ++id) this->limit(id, Side::Sell, 100 + static_cast<Price>(id), 1);
  auto r1 = this->limit(100, Side::Buy, 200, 3);
  EXPECT_EQ(r1.first_trade_seq, 1u);
  EXPECT_EQ(r1.trade_count, 3u);
  auto r2 = this->limit(101, Side::Buy, 200, 3);
  EXPECT_EQ(r2.first_trade_seq, 4u);
  EXPECT_EQ(this->book.last_seq(), 6u);
  for (size_t i = 0; i < this->trades.size(); ++i) EXPECT_EQ(this->trades[i].seq, i + 1);
}

TYPED_TEST(OrderBookTest, HighVolumeSequenceStaysConsistent) {
  // 20k orders alternating sides around a drifting mid; cancels every 3rd.
  uint64_t x = 88172645463325252ull;
  auto rnd = [&]() { x ^= x << 13; x ^= x >> 7; x ^= x << 17; return x; };
  Price mid = 5000;
  std::vector<OrderId> live;
  for (OrderId id = 1; id <= 20000; ++id) {
    Side s = (rnd() & 1) ? Side::Buy : Side::Sell;
    Price p = mid + static_cast<Price>(rnd() % 41) - 20;
    Qty q = static_cast<Qty>(1 + rnd() % 50);
    auto r = this->limit(id, s, p, q);
    ASSERT_NE(r.status, OrderStatus::Rejected);
    if (r.status == OrderStatus::Accepted || r.status == OrderStatus::PartiallyFilled) live.push_back(id);
    if (id % 3 == 0 && !live.empty()) {
      size_t k = rnd() % live.size();
      this->book.cancel(live[k]);  // may already be filled; either outcome ok
      live[k] = live.back();
      live.pop_back();
    }
    if (id % 1000 == 0) mid += static_cast<Price>(rnd() % 7) - 3;
  }
  this->check();
  auto b = this->book.best_bid_ask();
  if (b.has_bid && b.has_ask) { EXPECT_LT(b.bid_price, b.ask_price); }
  EXPECT_GT(this->trades.size(), 1000u);
}
