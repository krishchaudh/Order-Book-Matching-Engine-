// Slab allocator for resting orders. Orders are addressed by 32-bit slot
// index (not pointer) so the whole order record fits in 32 bytes and two
// orders share one cache line. Storage is reserved up front; untouched pages
// are never committed by the kernel, so a large capacity is cheap.
#pragma once

#include <cassert>
#include <cstdint>
#include <vector>

#include "me/types.h"

namespace me {

struct alignas(32) Order {
  OrderId id;
  Price price;
  Qty remaining;
  uint32_t next;  // slot index of the next order at the same price (FIFO)
  uint32_t prev;
  Side side;
  uint8_t pad_[3];
};
static_assert(sizeof(Order) == 32, "Order must stay 32 bytes");

class OrderPool {
 public:
  explicit OrderPool(uint32_t capacity) : capacity_(capacity) { slots_.reserve(capacity); }

  // Returns kNil when the pool is exhausted.
  uint32_t allocate() {
    if (free_head_ != kNil) {
      uint32_t s = free_head_;
      free_head_ = slots_[s].next;
      --free_count_;
      return s;
    }
    if (slots_.size() == capacity_) return kNil;
    slots_.emplace_back();
    return static_cast<uint32_t>(slots_.size() - 1);
  }

  void release(uint32_t slot) {
    assert(slot < slots_.size());
    slots_[slot].next = free_head_;
    free_head_ = slot;
    ++free_count_;
  }

  Order& operator[](uint32_t slot) { return slots_[slot]; }
  const Order& operator[](uint32_t slot) const { return slots_[slot]; }

  // Drops every order; keeps reserved storage.
  void clear() {
    slots_.clear();
    free_head_ = kNil;
    free_count_ = 0;
  }

  uint32_t capacity() const { return capacity_; }
  size_t live() const { return slots_.size() - free_count_; }
  size_t high_water() const { return slots_.size(); }

 private:
  std::vector<Order> slots_;
  uint32_t capacity_;
  uint32_t free_head_ = kNil;
  size_t free_count_ = 0;
};

}  // namespace me
