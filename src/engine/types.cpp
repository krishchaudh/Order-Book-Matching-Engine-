#include "me/types.h"

namespace me {

const char* to_string(Side s) { return s == Side::Buy ? "BUY" : "SELL"; }
const char* to_string(OrderType t) { return t == OrderType::Limit ? "LIMIT" : "MARKET"; }

const char* to_string(OrderStatus s) {
  switch (s) {
    case OrderStatus::Rejected: return "REJECTED";
    case OrderStatus::Accepted: return "ACCEPTED";
    case OrderStatus::PartiallyFilled: return "PARTIALLY_FILLED";
    case OrderStatus::Filled: return "FILLED";
    case OrderStatus::Canceled: return "CANCELED";
  }
  return "?";
}

const char* to_string(RejectReason r) {
  switch (r) {
    case RejectReason::None: return "NONE";
    case RejectReason::InvalidOrderId: return "INVALID_ORDER_ID";
    case RejectReason::DuplicateOrderId: return "DUPLICATE_ORDER_ID";
    case RejectReason::InvalidQuantity: return "INVALID_QUANTITY";
    case RejectReason::InvalidPrice: return "INVALID_PRICE";
    case RejectReason::PriceOutOfRange: return "PRICE_OUT_OF_RANGE";
    case RejectReason::UnknownSymbol: return "UNKNOWN_SYMBOL";
    case RejectReason::UnknownOrder: return "UNKNOWN_ORDER";
    case RejectReason::BookFull: return "BOOK_FULL";
    case RejectReason::InvalidSide: return "INVALID_SIDE";
    case RejectReason::InvalidOrderType: return "INVALID_ORDER_TYPE";
    case RejectReason::WrongPartition: return "WRONG_PARTITION";
  }
  return "?";
}

}  // namespace me
