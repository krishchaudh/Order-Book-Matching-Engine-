// Bounded multi-producer / single-consumer queue (Vyukov-style per-slot
// sequence numbers). Lock-free; producers spin on a CAS, the consumer never
// contends with other consumers. Used to hand requests to shard threads.
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace me {

template <class T>
class MpscQueue {
 public:
  explicit MpscQueue(size_t capacity_pow2) : mask_(capacity_pow2 - 1), cells_(new Cell[capacity_pow2]) {
    for (size_t i = 0; i < capacity_pow2; ++i) cells_[i].seq.store(i, std::memory_order_relaxed);
  }

  // Returns false when full.
  bool try_push(const T& v) {
    size_t pos = tail_.load(std::memory_order_relaxed);
    Cell* c;
    while (true) {
      c = &cells_[pos & mask_];
      size_t seq = c->seq.load(std::memory_order_acquire);
      intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
      if (dif == 0) {
        if (tail_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) break;
      } else if (dif < 0) {
        return false;
      } else {
        pos = tail_.load(std::memory_order_relaxed);
      }
    }
    c->value = v;
    c->seq.store(pos + 1, std::memory_order_release);
    return true;
  }

  // Single consumer only.
  bool try_pop(T& out) {
    Cell* c = &cells_[head_ & mask_];
    size_t seq = c->seq.load(std::memory_order_acquire);
    intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(head_ + 1);
    if (dif < 0) return false;
    out = c->value;
    c->seq.store(head_ + mask_ + 1, std::memory_order_release);
    ++head_;
    return true;
  }

  size_t capacity() const { return mask_ + 1; }

 private:
  struct alignas(64) Cell {
    std::atomic<size_t> seq;
    T value;
  };
  const size_t mask_;
  std::unique_ptr<Cell[]> cells_;
  alignas(64) std::atomic<size_t> tail_{0};
  alignas(64) size_t head_{0};
};

}  // namespace me
