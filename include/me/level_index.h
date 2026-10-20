// Price-level containers for one side of a book. All three expose the same
// tiny interface so OrderBookT can be instantiated with any of them and the
// benchmark suite can compare them on identical workloads:
//
//   * MapLevelIndex       std::map<Price, Level>            (baseline)
//   * SortedVecLevelIndex sorted std::vector<Level>, best at back
//   * DenseLevelIndex     flat array indexed by price tick + hierarchical
//                         bitmap for O(1) best / next-best discovery (default)
//
// Interface:
//   bool   accepts(Price) const           price representable by this index
//   Level* get_or_create(Price)           level for resting an order
//   Level* find(Price)                    nullptr if no such level
//   void   erase(Level*)                  level must be empty
//   Level* best()                         nullptr if side empty
//   template<F> void for_each_best_first(size_t max_levels, F)
#pragma once

#include <cstdint>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <vector>

#include "me/memory.h"
#include "me/types.h"

namespace me {

struct Level {
  Price price;
  TotalQty total_qty;
  uint32_t head;  // slot index of the oldest order (first to execute)
  uint32_t tail;
  uint32_t count;
  uint32_t pad_;
};
static_assert(sizeof(Level) == 32, "Level must stay 32 bytes");

inline void init_level(Level& l, Price p) {
  l.price = p;
  l.total_qty = 0;
  l.head = l.tail = kNil;
  l.count = 0;
}

// ---------------------------------------------------------------------------
// Hierarchical bitmap: find highest set bit <= i / lowest set bit >= i in
// O(number of tiers) (4 tiers for 2^20 bits).
class HierBitmap {
 public:
  explicit HierBitmap(size_t bits) {
    size_t n = bits;
    do {
      n = (n + 63) / 64;
      tiers_.emplace_back(n, 0ull);
    } while (n > 1);
  }

  void set(size_t i) {
    for (auto& t : tiers_) {
      uint64_t& w = t[i >> 6];
      bool was_empty = (w == 0);
      w |= 1ull << (i & 63);
      if (!was_empty) return;
      i >>= 6;
    }
  }

  void clear(size_t i) {
    for (auto& t : tiers_) {
      uint64_t& w = t[i >> 6];
      w &= ~(1ull << (i & 63));
      if (w != 0) return;
      i >>= 6;
    }
  }

  bool test(size_t i) const { return (tiers_[0][i >> 6] >> (i & 63)) & 1u; }

  // Highest set index <= i, or -1.
  int64_t find_highest_le(size_t i) const {
    size_t tier = 0;
    size_t idx = i;
    while (true) {
      size_t w = idx >> 6, b = idx & 63;
      uint64_t word = tiers_[tier][w] & (~0ull >> (63 - b));
      if (word) {
        idx = (w << 6) + 63 - static_cast<size_t>(__builtin_clzll(word));
        break;
      }
      if (w == 0 || tier + 1 == tiers_.size()) return -1;
      idx = w - 1;
      ++tier;
    }
    while (tier > 0) {
      --tier;
      uint64_t word = tiers_[tier][idx];
      idx = (idx << 6) + 63 - static_cast<size_t>(__builtin_clzll(word));
    }
    return static_cast<int64_t>(idx);
  }

  // Lowest set index >= i, or -1.
  int64_t find_lowest_ge(size_t i) const {
    size_t tier = 0;
    size_t idx = i;
    while (true) {
      size_t w = idx >> 6, b = idx & 63;
      if (w >= tiers_[tier].size()) return -1;
      uint64_t word = tiers_[tier][w] & (~0ull << b);
      if (word) {
        idx = (w << 6) + static_cast<size_t>(__builtin_ctzll(word));
        break;
      }
      if (tier + 1 == tiers_.size()) return -1;
      idx = w + 1;
      ++tier;
    }
    while (tier > 0) {
      --tier;
      uint64_t word = tiers_[tier][idx];
      idx = (idx << 6) + static_cast<size_t>(__builtin_ctzll(word));
    }
    return static_cast<int64_t>(idx);
  }

 private:
  std::vector<std::vector<uint64_t>> tiers_;
};

// ---------------------------------------------------------------------------
template <bool kBid>
class DenseLevelIndex {
 public:
  explicit DenseLevelIndex(Price max_price)
      : cap_(static_cast<size_t>(max_price)),
        levels_(huge_zeroed<Level>(cap_)),
        bitmap_(cap_) {}

  bool accepts(Price p) const { return p > 0 && static_cast<size_t>(p) < cap_; }

  Level* get_or_create(Price p) {
    Level* l = &levels_[p];
    if (!bitmap_.test(static_cast<size_t>(p))) {
      init_level(*l, p);
      bitmap_.set(static_cast<size_t>(p));
      if (best_ < 0 || better(p, best_)) best_ = p;
    }
    return l;
  }

  Level* find(Price p) {
    if (!accepts(p) || !bitmap_.test(static_cast<size_t>(p))) return nullptr;
    return &levels_[p];
  }

  void erase(Level* l) {
    Price p = l->price;
    bitmap_.clear(static_cast<size_t>(p));
    if (p == best_) best_ = next_worse(p);
  }

  Level* best() { return best_ < 0 ? nullptr : &levels_[best_]; }
  bool empty() const { return best_ < 0; }

  void clear() {
    for (Price p = best_; p >= 0;) {
      Price next = next_worse(p);
      bitmap_.clear(static_cast<size_t>(p));
      p = next;
    }
    best_ = -1;
  }

  template <class F>
  void for_each_best_first(size_t max_levels, F&& f) const {
    Price p = best_;
    for (size_t n = 0; p >= 0 && n < max_levels; ++n) {
      f(levels_[p]);
      p = next_worse(p);
    }
  }

  size_t level_count() const {
    size_t n = 0;
    for (Price p = best_; p >= 0; p = next_worse(p)) ++n;
    return n;
  }

 private:
  static bool better(Price a, Price b) { return kBid ? a > b : a < b; }

  Price next_worse(Price p) const {
    if (kBid) return p == 0 ? -1 : bitmap_.find_highest_le(static_cast<size_t>(p) - 1);
    return bitmap_.find_lowest_ge(static_cast<size_t>(p) + 1);
  }

  size_t cap_;
  HugeArray<Level> levels_;
  HierBitmap bitmap_;
  Price best_ = -1;
};

// ---------------------------------------------------------------------------
template <bool kBid>
class MapLevelIndex {
 public:
  explicit MapLevelIndex(Price max_price) : max_price_(max_price) {}

  bool accepts(Price p) const { return p > 0 && p < max_price_; }

  Level* get_or_create(Price p) {
    auto it = map_.lower_bound(p);
    if (it == map_.end() || it->first != p) {
      it = map_.emplace_hint(it, p, Level{});
      init_level(it->second, p);
    }
    return &it->second;
  }

  Level* find(Price p) {
    auto it = map_.find(p);
    return it == map_.end() ? nullptr : &it->second;
  }

  void erase(Level* l) { map_.erase(l->price); }

  Level* best() { return map_.empty() ? nullptr : &map_.begin()->second; }
  bool empty() const { return map_.empty(); }
  void clear() { map_.clear(); }

  template <class F>
  void for_each_best_first(size_t max_levels, F&& f) const {
    size_t n = 0;
    for (auto it = map_.begin(); it != map_.end() && n < max_levels; ++it, ++n) f(it->second);
  }
  size_t level_count() const { return map_.size(); }

 private:
  using Cmp = std::conditional_t<kBid, std::greater<Price>, std::less<Price>>;
  Price max_price_;
  std::map<Price, Level, Cmp> map_;
};

// ---------------------------------------------------------------------------
// Levels kept sorted worst -> best so the best level is at the back and
// insert/erase near the touch are cheap.
template <bool kBid>
class SortedVecLevelIndex {
 public:
  explicit SortedVecLevelIndex(Price max_price) : max_price_(max_price) { v_.reserve(256); }

  bool accepts(Price p) const { return p > 0 && p < max_price_; }

  Level* get_or_create(Price p) {
    auto it = lower_bound(p);
    if (it == v_.end() || it->price != p) {
      it = v_.insert(it, Level{});
      init_level(*it, p);
    }
    return &*it;
  }

  Level* find(Price p) {
    auto it = lower_bound(p);
    return (it == v_.end() || it->price != p) ? nullptr : &*it;
  }

  void erase(Level* l) { v_.erase(v_.begin() + (l - v_.data())); }

  Level* best() { return v_.empty() ? nullptr : &v_.back(); }
  bool empty() const { return v_.empty(); }
  void clear() { v_.clear(); }

  template <class F>
  void for_each_best_first(size_t max_levels, F&& f) const {
    size_t n = 0;
    for (auto it = v_.rbegin(); it != v_.rend() && n < max_levels; ++it, ++n) f(*it);
  }
  size_t level_count() const { return v_.size(); }

 private:
  // Ordering key: "worse" first. For bids ascending price, for asks descending.
  static bool worse(Price a, Price b) { return kBid ? a < b : a > b; }

  std::vector<Level>::iterator lower_bound(Price p) {
    size_t lo = 0, hi = v_.size();
    while (lo < hi) {
      size_t mid = (lo + hi) / 2;
      if (worse(v_[mid].price, p)) lo = mid + 1; else hi = mid;
    }
    return v_.begin() + static_cast<std::ptrdiff_t>(lo);
  }

  Price max_price_;
  std::vector<Level> v_;
};

}  // namespace me
