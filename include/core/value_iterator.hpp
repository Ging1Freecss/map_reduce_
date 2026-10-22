#pragma once
#include <concepts>
#include <string_view>
namespace mr {
template <typename T>
concept IsValueIterator = requires(T &obj) {
  { obj.has_next() } -> std::same_as<bool>;
  { obj.next() } -> std::same_as<std::string_view>;
};
} // namespace mr