#include <gtest/gtest.h>

#include <atomic>
#include <thread>

#include "me/mpsc_queue.h"
#include "me/sharded_engine.h"

using namespace me;

TEST(MpscQueue, ManyProducersOneConsumer) {
  MpscQueue<uint64_t> q(1 << 10);
  const int producers = 4, per = 50000;
  std::vector<std::thread> ts;
  for (int p = 0; p < producers; ++p)
    ts.emplace_back([&, p] {
      for (int i = 0; i < per; ++i) {
        uint64_t v = (static_cast<uint64_t>(p) << 32) | static_cast<uint64_t>(i);
        while (!q.try_push(v)) std::this_thread::yield();
      }
    });
  std::vector<uint64_t> last(producers, 0);
  std::vector<int> count(producers, 0);
  int total = 0;
  uint64_t v;
  while (total < producers * per) {
    if (!q.try_pop(v)) { std::this_thread::yield(); continue; }
    int p = static_cast<int>(v >> 32);
    uint64_t i = v & 0xFFFFFFFFu;
    if (count[p] > 0) { ASSERT_EQ(i, last[p] + 1); }  // per-producer FIFO
    last[p] = i;
    ++count[p];
    ++total;
  }
  for (auto& t : ts) t.join();
  EXPECT_FALSE(q.try_pop(v));
}

TEST(ShardedEngine, RoutesSymbolsToOwnerAndMatches) {
  ShardedEngineConfig cfg;
  cfg.shards = 3;
  cfg.book = BookConfig{1 << 16, 1 << 16};
  ShardedEngine se(cfg);
  std::vector<std::string> syms = {"AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA"};
  for (auto& s : syms) se.add_symbol(s);
  se.start();

  for (auto& name : syms) {
    SymbolRef ref;
    ASSERT_TRUE(se.lookup(name, ref));
    EXPECT_EQ(ref.shard, partition_of(name, 3));
    Request r;
    r.type = RequestType::Submit;
    r.symbol = ref.local_id;
    r.order = NewOrder{1, 100, 10, Side::Sell, OrderType::Limit};
    se.call(ref.shard, r);
    EXPECT_EQ(r.submit_result.status, OrderStatus::Accepted);
    r.order = NewOrder{2, 100, 4, Side::Buy, OrderType::Limit};
    se.call(ref.shard, r);
    EXPECT_EQ(r.submit_result.status, OrderStatus::Filled);
    ASSERT_EQ(r.trades.size(), 1u);
    EXPECT_EQ(r.trades[0].maker_id, 1u);
    r.type = RequestType::BestBidAsk;
    se.call(ref.shard, r);
    EXPECT_EQ(r.bba.ask_qty, 6u);
    r.type = RequestType::Cancel;
    r.cancel_id = 1;
    se.call(ref.shard, r);
    EXPECT_TRUE(r.cancel_result.ok);
    EXPECT_EQ(r.cancel_result.canceled_qty, 6u);
  }
  se.stop();
}

TEST(ShardedEngine, ConcurrentClientsAcrossShardsStayConsistent) {
  ShardedEngineConfig cfg;
  cfg.shards = 4;
  cfg.book = BookConfig{1 << 16, 1 << 16};
  cfg.spin_before_sleep = std::chrono::microseconds(50);  // exercise the park/wake path
  ShardedEngine se(cfg);
  const int nsym = 16;
  std::vector<SymbolRef> refs;
  for (int i = 0; i < nsym; ++i) refs.push_back(se.add_symbol("S" + std::to_string(i)));
  se.start();

  const int clients = 8, ops = 20000;
  std::atomic<uint64_t> fills{0};
  std::vector<std::thread> ts;
  for (int c = 0; c < clients; ++c)
    ts.emplace_back([&, c] {
      Request r;
      uint64_t x = 1234567 + c;
      auto rnd = [&]() { x ^= x << 13; x ^= x >> 7; x ^= x << 17; return x; };
      for (int i = 0; i < ops; ++i) {
        const SymbolRef& ref = refs[rnd() % nsym];
        r.type = RequestType::Submit;
        r.symbol = ref.local_id;
        // Ids are unique per client; symbol scoping keeps them from colliding.
        r.order = NewOrder{static_cast<OrderId>(c) * 1000000 + i + 1,
                           static_cast<Price>(1000 + rnd() % 20), static_cast<Qty>(1 + rnd() % 10),
                           (rnd() & 1) ? Side::Buy : Side::Sell, OrderType::Limit};
        se.call(ref.shard, r);
        ASSERT_NE(r.submit_result.status, OrderStatus::Rejected);
        fills += r.submit_result.filled_qty;
        if (i % 5 == 0) std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    });
  for (auto& t : ts) t.join();
  se.stop();
  EXPECT_GT(fills.load(), 0u);
  uint64_t processed = 0;
  for (uint32_t s = 0; s < cfg.shards; ++s) {
    processed += se.processed(s);
    for (SymbolId id = 0; id < se.engine(s).symbol_count(); ++id) {
      std::string why;
      EXPECT_TRUE(se.engine(s).book(id).check_invariants(&why)) << why;
    }
  }
  EXPECT_EQ(processed, static_cast<uint64_t>(clients) * ops);
}

TEST(ShardedEngine, RestartAfterStop) {
  me::ShardedEngineConfig cfg;
  cfg.shards = 2;
  me::ShardedEngine se(cfg);
  auto ref = se.add_symbol("RST");
  for (int round = 0; round < 3; ++round) {
    se.start();
    me::Request r;
    r.type = me::RequestType::Submit;
    r.symbol = ref.local_id;
    r.order = me::NewOrder{static_cast<me::OrderId>(round + 1), 100, 1, me::Side::Buy, me::OrderType::Limit};
    se.call(ref.shard, r);
    EXPECT_EQ(r.submit_result.status, me::OrderStatus::Accepted);
    se.stop();
  }
  EXPECT_EQ(se.engine(ref.shard).open_orders(), 3u);
}
