#pragma once

#include "core/emit_context.hpp"
#include "core/value_iterator.hpp"
#include <concepts>
#include <string_view>

namespace mr {
template <typename T, typename Iter, typename Emitter>
concept IsReducer =
    IsValueIterator<Iter> &&
    IsEmitter<Emitter> &&
    requires(T &reducer, std::string_view key, Iter &values, Emitter &ctx) {
      { reducer.reduce(key, values, ctx) } -> std::same_as<void>;
    };
} // namespace mr