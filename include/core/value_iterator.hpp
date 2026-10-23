#pragma once
#include <concepts>
#include <cstddef>
#include <string_view>
namespace mr {
template <typename T>
concept IsValueIterator = requires(T &obj) {
  { obj.has_next() } -> std::same_as<bool>;
  { obj.next() } -> std::same_as<std::string_view>;
  { obj.current_index() } -> std::same_as<std::size_t>;
};
} // namespace mr