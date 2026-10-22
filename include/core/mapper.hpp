#pragma once

#include "core/reducer.hpp"
#include "storage/error.hpp"
#include <concepts>
#include <expected>
#include <string_view>
namespace mr {
template <typename T, typename emitter>
concept IsMapper =
    IsEmitter<emitter> && requires(T &mapper, std::string_view key,
                                   std::string_view value, emitter &e) {
      {
        mapper.map(key, value, e)
      } -> std::same_as<std::expected<void, StorageError>>;
    };
} // namespace mr
