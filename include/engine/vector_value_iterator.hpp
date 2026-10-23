#pragma once

#include "core/value_iterator.hpp"
#include <cstddef>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace mr {
class VectorValueIterator {
private:
  const std::vector<std::pair<std::string, std::string>> &sorted_vec_;
  std::string_view current_key_{""};
  std::size_t index_{0};

public:
  VectorValueIterator(
      const std::vector<std::pair<std::string, std::string>> &sorted_vec,
      std::string_view current_key, std::size_t index)
      : sorted_vec_(sorted_vec), current_key_(current_key), index_(index) {}

  auto has_next() -> bool {
    if (index_ < sorted_vec_.size() &&
        current_key_ == sorted_vec_[index_].first) {
      return true;
    }
    return false;
  }

  auto next() -> std::string_view { return sorted_vec_[index_++].second; }

  // return the next unique key index
  auto current_index() -> std::size_t {

    while (index_ < sorted_vec_.size() &&
           sorted_vec_[index_].first == current_key_) {
      index_++;
    }
    return index_;
  }
};

static_assert(IsValueIterator<VectorValueIterator>,
              "class is not a value iterator");
} // namespace mr
