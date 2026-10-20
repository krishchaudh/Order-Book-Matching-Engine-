// Zeroed, lazily-committed, huge-page-friendly allocation for the large flat
// tables (order-id index, dense price levels). mmap gives zero pages that are
// only committed on first touch; MADV_HUGEPAGE lets THP=madvise systems back
// hot tables with 2 MiB pages (fewer dTLB misses / page faults per insert).
#pragma once

#include <sys/mman.h>

#include <cstddef>
#include <cstdlib>
#include <memory>

namespace me {

struct HugeFree {
  size_t bytes = 0;
  void operator()(void* p) const {
    if (p) ::munmap(p, bytes);
  }
};

template <typename T>
using HugeArray = std::unique_ptr<T[], HugeFree>;

// Returns zero-filled storage for n elements (n may be 0 -> nullptr).
template <typename T>
HugeArray<T> huge_zeroed(size_t n) {
  if (n == 0) return HugeArray<T>(nullptr, HugeFree{0});
  constexpr size_t kHuge = size_t(2) << 20;
  size_t bytes = n * sizeof(T);
  if (bytes >= kHuge) bytes = (bytes + kHuge - 1) & ~(kHuge - 1);
  void* p = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  if (p == MAP_FAILED) std::abort();
#ifdef MADV_HUGEPAGE
  if (bytes >= kHuge) ::madvise(p, bytes, MADV_HUGEPAGE);
#endif
  return HugeArray<T>(static_cast<T*>(p), HugeFree{bytes});
}

}  // namespace me
