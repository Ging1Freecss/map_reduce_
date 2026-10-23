#pragma once

#include "core/emit_context.hpp"
#include "core/reducer.hpp"
#include "core/value_iterator.hpp"
#include "engine/vector_value_iterator.hpp"
#include "storage/error.hpp"
#include "storage/record_reader.hpp"
#include "storage/staged_record_writer.hpp"
#include <algorithm>
#include <cstddef>
#include <expected>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
namespace mr {

class ReduceEmitContext {
private:
  std::size_t reduce_id_;
  std::filesystem::path base_dir_;
  StagedRecordWriter out_;

public:
  using ErrorState = StorageError;
  explicit ReduceEmitContext(std::size_t reduce_id,
                             std::filesystem::path base_dir,
                             std::size_t buffer_size = 64 * 1024)
      : reduce_id_(reduce_id), base_dir_(base_dir),
        out_{base_dir_ /
                 std::filesystem::path("mr-out-" + std::to_string(reduce_id_)),
             buffer_size} {}

  auto emit(std::string_view key, std::string_view value)
      -> std::expected<void, ErrorState> {
    return out_.write_record(key, value);
  }

  auto commit() -> std::expected<void, ErrorState> { return out_.commit(); }
};

static_assert(IsEmitter<ReduceEmitContext>, "the class is not an emitter");

template <IsValueIterator Iter = VectorValueIterator,
          IsEmitter Emitter = ReduceEmitContext>
class WordCountReducer {
public:
  using ErrorState = StorageError;
  auto reduce(std::string_view key, Iter &values, Emitter &ctx)
      -> std::expected<void, ErrorState> {

    std::size_t sum{0};
    while (values.has_next()) {
      sum += std::stoul(std::string(values.next()));
    }

    return ctx.emit(key, std::to_string(sum));
  }
};

static_assert(
    IsReducer<WordCountReducer<VectorValueIterator, ReduceEmitContext>,
              VectorValueIterator, ReduceEmitContext>,
    "Reducer class is not implemented properly");

template <IsValueIterator Iter = VectorValueIterator,
          IsEmitter Emitter = ReduceEmitContext>
inline auto run_reduce_phase(std::filesystem::path base_dir,
                             std::size_t reduce_id, std::size_t num_maps,
                             IsReducer<Iter, Emitter> auto &reducer,
                             std::size_t buffer_size = 64 * 1024)
    -> std::expected<void, StorageError> {

  std::vector<std::pair<std::string, std::string>> record;

  for (std::size_t m = 0; m < num_maps; m++) {
    std::string filename =
        "mr-" + std::to_string(m) + "_" + std::to_string(reduce_id) + ".data";

    std::ifstream in(base_dir / std::filesystem::path(filename));

    if (!in.is_open()) {
      return std::unexpected(StorageError::IoError);
    }

    RecordReader reader{in};

    std::string out_key{""};
    std::string out_value{""};
    while (true) {
      auto res = reader.next_record(out_key, out_value);
      if (!res)
        return std::unexpected(res.error());

      if (!res.value()) {
        // EOF
        break;
      }
      record.emplace_back(std::move(out_key), std::move(out_value));
    }
  }
  std::sort(record.begin(), record.end(),
            [](const std::pair<std::string, std::string> &a,
               const std::pair<std::string, std::string> &b) -> bool {
              return a < b;
            });

  ReduceEmitContext reduce_ctx{reduce_id, base_dir, buffer_size};
  std::size_t idx{0};

  while (idx < record.size()) {
    std::string_view key = record[idx].first;
    VectorValueIterator iter_{record, key, idx};

    auto res = reducer.reduce(key, iter_, reduce_ctx);

    if (!res) // io error
      return std::unexpected(res.error());

    idx = iter_.current_index();
  }

  auto res = reduce_ctx.commit();
  return res;
}
} // namespace mr