#include <gtest/gtest.h>

#include "me/engine.h"

using namespace me;

TEST(Engine, SymbolsAreIndependent) {
  Engine eng(BookConfig{1 << 16, 1 << 16});
  SymbolId a = eng.add_symbol("AAPL");
  SymbolId b = eng.add_symbol("MSFT");
  EXPECT_NE(a, b);
  EXPECT_EQ(eng.add_symbol("AAPL"), a);
  EXPECT_EQ(eng.symbol_count(), 2u);
  SymbolId found;
  EXPECT_TRUE(eng.lookup("MSFT", found));
  EXPECT_EQ(found, b);
  EXPECT_FALSE(eng.lookup("GOOG", found));

  std::vector<Trade> trades;
  // Same order id on two symbols is fine: ids are scoped per symbol.
  EXPECT_EQ(eng.submit(a, NewOrder{1, 100, 10, Side::Sell, OrderType::Limit}, trades).status, OrderStatus::Accepted);
  EXPECT_EQ(eng.submit(b, NewOrder{1, 100, 10, Side::Sell, OrderType::Limit}, trades).status, OrderStatus::Accepted);
  // A buy on MSFT does not touch AAPL liquidity.
  auto r = eng.submit(b, NewOrder{2, 100, 10, Side::Buy, OrderType::Limit}, trades);
  EXPECT_EQ(r.status, OrderStatus::Filled);
  EXPECT_EQ(eng.book(a).open_orders(), 1u);
  EXPECT_EQ(eng.book(b).open_orders(), 0u);
  EXPECT_EQ(eng.open_orders(), 1u);
  // Sequence numbers are per book.
  EXPECT_EQ(eng.book(b).last_seq(), 1u);
  EXPECT_EQ(eng.book(a).last_seq(), 0u);
}

TEST(Engine, UnknownSymbolRejected) {
  Engine eng;
  std::vector<Trade> trades;
  auto r = eng.submit(5, NewOrder{1, 100, 10, Side::Sell, OrderType::Limit}, trades);
  EXPECT_EQ(r.status, OrderStatus::Rejected);
  EXPECT_EQ(r.reason, RejectReason::UnknownSymbol);
  auto c = eng.cancel(5, 1);
  EXPECT_FALSE(c.ok);
  EXPECT_EQ(c.reason, RejectReason::UnknownSymbol);
}

TEST(Engine, BookFullIsReportedNotCrashed) {
  Engine eng(BookConfig{1 << 10, 4});
  SymbolId s = eng.add_symbol("X");
  std::vector<Trade> trades;
  for (OrderId id = 1; id <= 4; ++id)
    EXPECT_EQ(eng.submit(s, NewOrder{id, 10, 1, Side::Buy, OrderType::Limit}, trades).status, OrderStatus::Accepted);
  auto r = eng.submit(s, NewOrder{5, 10, 1, Side::Buy, OrderType::Limit}, trades);
  EXPECT_EQ(r.status, OrderStatus::Rejected);
  EXPECT_EQ(r.reason, RejectReason::BookFull);
  EXPECT_EQ(eng.book(s).open_orders(), 4u);
  // Freed slots are reused.
  EXPECT_TRUE(eng.cancel(s, 2).ok);
  EXPECT_EQ(eng.submit(s, NewOrder{5, 10, 1, Side::Buy, OrderType::Limit}, trades).status, OrderStatus::Accepted);
  EXPECT_TRUE(eng.book(s).check_invariants());
}
