// Multi-symbol engine: a symbol registry plus one OrderBook per symbol.
// An Engine instance is single-writer: exactly one thread may mutate it.
// Concurrency across symbols is achieved by sharding symbols over several
// Engine instances (see sharded_engine.h), never by locking inside a book.
#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "me/order_book.h"
#include "me/types.h"

namespace me {

template <class BookT>
class EngineT {
 public:
  explicit EngineT(BookConfig cfg = BookConfig{}) : cfg_(cfg) {}

  // Registers a symbol; returns its id. Re-registering returns the existing id.
  SymbolId add_symbol(const std::string& name) {
    auto it = by_name_.find(name);
    if (it != by_name_.end()) return it->second;
    SymbolId id = static_cast<SymbolId>(books_.size());
    books_.emplace_back(std::make_unique<BookT>(cfg_));
    names_.push_back(name);
    by_name_.emplace(name, id);
    return id;
  }

  bool lookup(const std::string& name, SymbolId& out) const {
    auto it = by_name_.find(name);
    if (it == by_name_.end()) return false;
    out = it->second;
    return true;
  }

  bool has_symbol(SymbolId s) const { return s < books_.size(); }
  size_t symbol_count() const { return books_.size(); }
  const std::string& symbol_name(SymbolId s) const { return names_[s]; }

  BookT& book(SymbolId s) { return *books_[s]; }
  const BookT& book(SymbolId s) const { return *books_[s]; }

  SubmitResult submit(SymbolId s, const NewOrder& o, std::vector<Trade>& trades) {
    if (ME_UNLIKELY(s >= books_.size())) {
      SubmitResult r;
      r.status = OrderStatus::Rejected;
      r.reason = RejectReason::UnknownSymbol;
      r.remaining_qty = o.qty;
      return r;
    }
    return books_[s]->submit(o, trades);
  }

  CancelResult cancel(SymbolId s, OrderId id) {
    if (ME_UNLIKELY(s >= books_.size())) {
      CancelResult r;
      r.reason = RejectReason::UnknownSymbol;
      return r;
    }
    return books_[s]->cancel(id);
  }

  size_t open_orders() const {
    size_t n = 0;
    for (auto& b : books_) n += b->open_orders();
    return n;
  }

 private:
  BookConfig cfg_;
  std::vector<std::unique_ptr<BookT>> books_;
  std::vector<std::string> names_;
  std::unordered_map<std::string, SymbolId> by_name_;
};

using Engine = EngineT<OrderBook>;

}  // namespace me
