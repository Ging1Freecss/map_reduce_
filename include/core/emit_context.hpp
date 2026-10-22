#pragma once
#include "storage/error.hpp"
#include <concepts>
#include <expected>
#include <string_view>
namespace mr {
using ErrorState = StorageError;
template <typename T>
concept IsEmitter = requires(T &obj, std::string_view a, std::string_view b) {
  { obj.emit(a, b) } -> std::same_as<std::expected<void, ErrorState>>;
};

} // namespace mr