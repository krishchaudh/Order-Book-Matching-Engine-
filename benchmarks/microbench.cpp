// Google Benchmark micro-benchmarks comparing the data-structure alternatives
// in isolation: order-id index (open addressing vs std::unordered_map) and
// price-level index (dense+bitmap vs std::map vs sorted vector).
#include <benchmark/benchmark.h>

#include <random>
#include <vector>

#include "me/level_index.h"
#include "me/order_index.h"
#include "me/order_book.h"

using namespace me;

template <class Index>
static void BM_IndexInsertFind(benchmark::State& st) {
  const size_t n = static_cast<size_t>(st.range(0));
  std::vector<OrderId> ids(n);
  std::mt19937_64 rng(1);
  for (auto& id : ids) id = (rng() >> 1) | 1;
  for (auto _ : st) {
    Index idx(n * 2);
    for (size_t i = 0; i < n; ++i) idx.insert(ids[i], static_cast<uint32_t>(i));
    uint32_t sum = 0;
    for (size_t i = 0; i < n; ++i) sum += idx.find(ids[i]);
    benchmark::DoNotOptimize(sum);
    for (size_t i = 0; i < n; ++i) idx.erase(ids[i]);
  }
  st.SetItemsProcessed(static_cast<int64_t>(st.iterations() * n * 3));
}
BENCHMARK_TEMPLATE(BM_IndexInsertFind, OpenAddressOrderIndex)->Arg(1000)->Arg(100000)->Arg(1000000);
BENCHMARK_TEMPLATE(BM_IndexInsertFind, StdOrderIndex)->Arg(1000)->Arg(100000)->Arg(1000000);

template <class LevelIdx>
static void BM_LevelBestAfterChurn(benchmark::State& st) {
  const Price levels = st.range(0);
  LevelIdx idx(1 << 20);
  std::mt19937_64 rng(2);
  for (auto _ : st) {
    Price base = 500000;
    for (Price i = 0; i < levels; ++i) idx.get_or_create(base + i);
    Price acc = 0;
    for (int k = 0; k < 1000; ++k) {
      Level* b = idx.best();
      acc += b->price;
      idx.erase(b);
      idx.get_or_create(base + static_cast<Price>(rng() % static_cast<uint64_t>(levels)));
    }
    benchmark::DoNotOptimize(acc);
    while (Level* b = idx.best()) idx.erase(b);
  }
  st.SetItemsProcessed(static_cast<int64_t>(st.iterations() * 1000));
}
BENCHMARK_TEMPLATE(BM_LevelBestAfterChurn, DenseLevelIndex<true>)->Arg(16)->Arg(1024)->Arg(65536);
BENCHMARK_TEMPLATE(BM_LevelBestAfterChurn, MapLevelIndex<true>)->Arg(16)->Arg(1024)->Arg(65536);
BENCHMARK_TEMPLATE(BM_LevelBestAfterChurn, SortedVecLevelIndex<true>)->Arg(16)->Arg(1024)->Arg(65536);

template <class Book>
static void BM_BookInsertCancel(benchmark::State& st) {
  Book book(BookConfig{});
  std::vector<Trade> trades;
  std::mt19937_64 rng(3);
  OrderId id = 1;
  const Price spread = 50;
  for (auto _ : st) {
    OrderId first = id;
    for (int i = 0; i < 1000; ++i, ++id) {
      Side s = (rng() & 1) ? Side::Buy : Side::Sell;
      Price px = s == Side::Buy ? 10000 - 1 - static_cast<Price>(rng() % spread) : 10001 + static_cast<Price>(rng() % spread);
      book.submit(NewOrder{id, px, 10, s, OrderType::Limit}, trades);
    }
    for (OrderId c = first; c < id; ++c) book.cancel(c);
  }
  st.SetItemsProcessed(static_cast<int64_t>(st.iterations() * 2000));
}
BENCHMARK_TEMPLATE(BM_BookInsertCancel, OrderBook);
BENCHMARK_TEMPLATE(BM_BookInsertCancel, DenseOrderBookStd);
BENCHMARK_TEMPLATE(BM_BookInsertCancel, MapOrderBookOA);
BENCHMARK_TEMPLATE(BM_BookInsertCancel, MapOrderBook);
BENCHMARK_TEMPLATE(BM_BookInsertCancel, SortedVecOrderBook);

BENCHMARK_MAIN();
