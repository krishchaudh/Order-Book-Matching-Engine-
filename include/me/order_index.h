// OrderId -> pool slot lookup. Two interchangeable implementations:
//   * OpenAddressOrderIndex: flat open-addressing table, linear probing,
//     Fibonacci hashing, backward-shift deletion (no tombstones). 16-byte
//     entries, max load factor 0.5.
//   * StdOrderIndex: std::unordered_map adapter, kept as the baseline.
#pragma once

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <unordered_map>

#include "me/types.h"

namespace me {

class OpenAddressOrderIndex {
 public:
  explicit OpenAddressOrderIndex(size_t expected = 1024) {
    size_t cap = 16;
    while (cap < expected * 2) cap <<= 1;
    rehash(cap);
  }

  // Returns false if key already present.
  bool insert(OrderId key, uint32_t value) {
    if ((size_ + 1) * 2 > cap_) rehash(cap_ * 2);
    size_t i = slot_of(key);
    while (true) {
      Entry& e = table_[i];
      if (e.key == 0) {
        e.key = key;
        e.value = value;
        ++size_;
        return true;
      }
      if (e.key == key) return false;
      i = (i + 1) & mask_;
    }
  }

  // Returns kNil when absent.
  uint32_t find(OrderId key) const {
    size_t i = slot_of(key);
    while (true) {
      const Entry& e = table_[i];
      if (e.key == key) return e.value;
      if (e.key == 0) return kNil;
      i = (i + 1) & mask_;
    }
  }

  bool contains(OrderId key) const { return find(key) != kNil; }

  // Removes key; returns kNil when absent, else the stored value.
  uint32_t erase(OrderId key) {
    size_t i = slot_of(key);
    while (true) {
      Entry& e = table_[i];
      if (e.key == 0) return kNil;
      if (e.key == key) break;
      i = (i + 1) & mask_;
    }
    uint32_t value = table_[i].value;
    // Backward-shift deletion keeps probe chains intact without tombstones.
    size_t hole = i;
    size_t j = i;
    while (true) {
      j = (j + 1) & mask_;
      if (table_[j].key == 0) break;
      size_t home = slot_of(table_[j].key);
      bool in_range = hole <= j ? (hole < home && home <= j) : (hole < home || home <= j);
      if (in_range) continue;
      table_[hole] = table_[j];
      hole = j;
    }
    table_[hole].key = 0;
    --size_;
    return value;
  }

  void clear() {
    std::memset(table_.get(), 0, cap_ * sizeof(Entry));
    size_ = 0;
  }

  size_t size() const { return size_; }
  size_t capacity() const { return cap_; }

 private:
  struct Entry {
    OrderId key;  // 0 == empty (OrderId 0 is rejected by the book)
    uint32_t value;
    uint32_t pad_;
  };

  size_t slot_of(OrderId key) const {
    return static_cast<size_t>((key * 0x9E3779B97F4A7C15ull) >> shift_);
  }

  void rehash(size_t new_cap) {
    std::unique_ptr<Entry[], void (*)(void*)> old(table_.release(), std::free);
    size_t old_cap = cap_;
    table_.reset(static_cast<Entry*>(std::calloc(new_cap, sizeof(Entry))));
    cap_ = new_cap;
    mask_ = new_cap - 1;
    shift_ = 64;
    for (size_t c = new_cap; c > 1; c >>= 1) --shift_;
    size_ = 0;
    for (size_t i = 0; i < old_cap; ++i)
      if (old[i].key != 0) insert(old[i].key, old[i].value);
  }

  std::unique_ptr<Entry[], void (*)(void*)> table_{nullptr, std::free};
  size_t cap_ = 0;
  size_t mask_ = 0;
  unsigned shift_ = 64;
  size_t size_ = 0;
};

class StdOrderIndex {
 public:
  explicit StdOrderIndex(size_t expected = 1024) { map_.reserve(expected); }
  bool insert(OrderId key, uint32_t value) { return map_.emplace(key, value).second; }
  uint32_t find(OrderId key) const {
    auto it = map_.find(key);
    return it == map_.end() ? kNil : it->second;
  }
  bool contains(OrderId key) const { return map_.count(key) != 0; }
  uint32_t erase(OrderId key) {
    auto it = map_.find(key);
    if (it == map_.end()) return kNil;
    uint32_t v = it->second;
    map_.erase(it);
    return v;
  }
  void clear() { map_.clear(); }
  size_t size() const { return map_.size(); }

 private:
  std::unordered_map<OrderId, uint32_t> map_;
};

}  // namespace me
