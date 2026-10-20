#include <gtest/gtest.h>

#include <random>
#include <unordered_map>

#include "me/order_index.h"

using namespace me;

TEST(OpenAddressOrderIndex, InsertFindErase) {
  OpenAddressOrderIndex idx(4);
  EXPECT_TRUE(idx.insert(10, 1));
  EXPECT_TRUE(idx.insert(20, 2));
  EXPECT_FALSE(idx.insert(10, 3));
  EXPECT_EQ(idx.find(10), 1u);
  EXPECT_EQ(idx.find(20), 2u);
  EXPECT_EQ(idx.find(30), kNil);
  EXPECT_EQ(idx.erase(10), 1u);
  EXPECT_EQ(idx.erase(10), kNil);
  EXPECT_EQ(idx.find(10), kNil);
  EXPECT_EQ(idx.find(20), 2u);
  EXPECT_EQ(idx.size(), 1u);
}

TEST(OpenAddressOrderIndex, GrowsAndKeepsEntries) {
  OpenAddressOrderIndex idx(2);
  for (OrderId k = 1; k <= 100000; ++k) ASSERT_TRUE(idx.insert(k, static_cast<uint32_t>(k * 3)));
  for (OrderId k = 1; k <= 100000; ++k) ASSERT_EQ(idx.find(k), static_cast<uint32_t>(k * 3));
  EXPECT_EQ(idx.size(), 100000u);
  EXPECT_GE(idx.capacity(), 200000u);
}

TEST(OpenAddressOrderIndex, RandomizedAgainstUnorderedMap) {
  OpenAddressOrderIndex idx(16);
  std::unordered_map<OrderId, uint32_t> ref;
  std::mt19937_64 rng(7);
  for (int i = 0; i < 500000; ++i) {
    // Small key space + colliding keys exercise backward-shift deletion.
    OrderId k = 1 + (rng() % 4096) * ((rng() & 1) ? 1 : 65536);
    int op = rng() % 3;
    if (op == 0) {
      uint32_t v = static_cast<uint32_t>(rng());
      bool a = idx.insert(k, v);
      bool b = ref.emplace(k, v).second;
      ASSERT_EQ(a, b);
    } else if (op == 1) {
      uint32_t a = idx.erase(k);
      auto it = ref.find(k);
      uint32_t b = it == ref.end() ? kNil : it->second;
      if (it != ref.end()) ref.erase(it);
      ASSERT_EQ(a, b);
    } else {
      auto it = ref.find(k);
      ASSERT_EQ(idx.find(k), it == ref.end() ? kNil : it->second);
    }
    ASSERT_EQ(idx.size(), ref.size());
  }
}
