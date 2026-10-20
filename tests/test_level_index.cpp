#include <gtest/gtest.h>

#include <random>
#include <set>

#include "me/level_index.h"

using namespace me;

TEST(HierBitmap, BasicSetClearFind) {
  HierBitmap bm(1 << 20);
  EXPECT_EQ(bm.find_highest_le((1 << 20) - 1), -1);
  EXPECT_EQ(bm.find_lowest_ge(0), -1);
  bm.set(5);
  bm.set(70);
  bm.set(100000);
  EXPECT_TRUE(bm.test(5));
  EXPECT_FALSE(bm.test(6));
  EXPECT_EQ(bm.find_lowest_ge(0), 5);
  EXPECT_EQ(bm.find_lowest_ge(5), 5);
  EXPECT_EQ(bm.find_lowest_ge(6), 70);
  EXPECT_EQ(bm.find_lowest_ge(71), 100000);
  EXPECT_EQ(bm.find_lowest_ge(100001), -1);
  EXPECT_EQ(bm.find_highest_le((1 << 20) - 1), 100000);
  EXPECT_EQ(bm.find_highest_le(99999), 70);
  EXPECT_EQ(bm.find_highest_le(69), 5);
  EXPECT_EQ(bm.find_highest_le(4), -1);
  bm.clear(70);
  EXPECT_EQ(bm.find_lowest_ge(6), 100000);
  EXPECT_EQ(bm.find_highest_le(99999), 5);
  bm.clear(5);
  bm.clear(100000);
  EXPECT_EQ(bm.find_lowest_ge(0), -1);
}

TEST(HierBitmap, RandomizedAgainstStdSet) {
  const size_t N = 1 << 14;
  HierBitmap bm(N);
  std::set<size_t> ref;
  std::mt19937_64 rng(42);
  for (int iter = 0; iter < 200000; ++iter) {
    size_t i = rng() % N;
    int op = rng() % 3;
    if (op == 0) { bm.set(i); ref.insert(i); }
    else if (op == 1) { bm.clear(i); ref.erase(i); }
    else {
      auto it = ref.lower_bound(i);
      int64_t exp_ge = it == ref.end() ? -1 : static_cast<int64_t>(*it);
      EXPECT_EQ(bm.find_lowest_ge(i), exp_ge);
      auto it2 = ref.upper_bound(i);
      int64_t exp_le = it2 == ref.begin() ? -1 : static_cast<int64_t>(*std::prev(it2));
      EXPECT_EQ(bm.find_highest_le(i), exp_le);
    }
  }
}

template <class Idx>
class LevelIndexTest : public ::testing::Test {};
using BidIdx = ::testing::Types<DenseLevelIndex<true>, MapLevelIndex<true>, SortedVecLevelIndex<true>>;
TYPED_TEST_SUITE(LevelIndexTest, BidIdx);

TYPED_TEST(LevelIndexTest, BidBestIsHighest) {
  TypeParam idx(1 << 16);
  EXPECT_EQ(idx.best(), nullptr);
  idx.get_or_create(100)->count = 1;
  idx.get_or_create(105)->count = 1;
  idx.get_or_create(90)->count = 1;
  ASSERT_NE(idx.best(), nullptr);
  EXPECT_EQ(idx.best()->price, 105);
  std::vector<Price> seen;
  idx.for_each_best_first(10, [&](const Level& l) { seen.push_back(l.price); });
  EXPECT_EQ(seen, (std::vector<Price>{105, 100, 90}));
  idx.erase(idx.find(105));
  EXPECT_EQ(idx.best()->price, 100);
  EXPECT_EQ(idx.find(105), nullptr);
  EXPECT_EQ(idx.get_or_create(100), idx.find(100));
  idx.erase(idx.find(90));
  idx.erase(idx.find(100));
  EXPECT_EQ(idx.best(), nullptr);
  EXPECT_EQ(idx.level_count(), 0u);
}

TEST(LevelIndex, AskBestIsLowest) {
  DenseLevelIndex<false> d(1 << 16);
  MapLevelIndex<false> m(1 << 16);
  SortedVecLevelIndex<false> v(1 << 16);
  for (Price p : {300, 250, 275, 1000}) { d.get_or_create(p); m.get_or_create(p); v.get_or_create(p); }
  EXPECT_EQ(d.best()->price, 250);
  EXPECT_EQ(m.best()->price, 250);
  EXPECT_EQ(v.best()->price, 250);
  d.erase(d.find(250)); m.erase(m.find(250)); v.erase(v.find(250));
  EXPECT_EQ(d.best()->price, 275);
  EXPECT_EQ(m.best()->price, 275);
  EXPECT_EQ(v.best()->price, 275);
  EXPECT_FALSE(d.accepts(0));
  EXPECT_FALSE(d.accepts(1 << 16));
  EXPECT_TRUE(d.accepts((1 << 16) - 1));
}
