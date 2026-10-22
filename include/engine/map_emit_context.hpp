
#include "core/emit_context.hpp"
#include "core/mapper.hpp"
#include "core/partitioner.hpp"
#include "core/reducer.hpp"
#include "storage/error.hpp"
#include "storage/staged_record_writer.hpp"
#include <cassert>
#include <cctype>
#include <cstddef>
#include <expected>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace mr {

template <IsPartitioner P = HashPartitioner> class MapEmitContext {
public:
  using ErrorState = StorageError;

  auto emit(std::string_view key, std::string_view value)
      -> std::expected<void, ErrorState> {

    std::size_t index = partitioner_.partition(key, num_reducer_);
    return writer_[index]->write_record(key, value);
  }

  auto commit() -> std::expected<void, ErrorState> {
    for (std::size_t i = 0; i < num_reducer_; i++) {
      std::expected<void, ErrorState> res = writer_[i]->commit();

      if (!res) {
        return res;
      }
    }

    return {};
  }

  MapEmitContext(std::size_t map_id, std::size_t num_reducer,
                 const std::filesystem::path &base_dir, P partition = P{},
                 std::size_t buffer_size = 64 * 1024)
      : map_id_(map_id), num_reducer_(num_reducer), base_dir_(base_dir),
        partitioner_(partition) {
    assert(num_reducer > 0 && "num_reducer must be greater than 0");

    writer_.reserve(num_reducer_);
    for (std::size_t i = 0; i < num_reducer_; i++) {

      std::string filename =
          "mr-" + std::to_string(map_id_) + "_" + std::to_string(i) + ".data";

      std::filesystem::path partition_path =
          base_dir_ / std::filesystem::path{filename};

      writer_.emplace_back(
          std::make_unique<StagedRecordWriter>(partition_path, buffer_size));
    }
  }

private:
  std::size_t map_id_{0};
  std::size_t num_reducer_{0};
  std::vector<std::unique_ptr<StagedRecordWriter>> writer_;
  std::filesystem::path base_dir_;
  P partitioner_;
};

static_assert(IsEmitter<MapEmitContext<HashPartitioner>>,
              "class is not a emitter");

class WordCountMapper {
public:
  auto map([[maybe_unused]] std::string_view key, std::string_view value,
           IsEmitter auto &ctx) -> std::expected<void, ErrorState> {

    std::size_t value_length = value.length();
    std::size_t cursor = 0;
    while (cursor < value_length) {

      while (cursor < value_length &&
             std::isspace(static_cast<unsigned char>(value[cursor]))) {
        cursor++;
      }

      if (cursor >= value_length) {
        break;
      }

      std::size_t end = cursor;
      while (end < value_length &&
             !std::isspace(static_cast<unsigned char>(value[end]))) {
        end++;
      }

      std::string_view token = value.substr(cursor, end - cursor);
      cursor = end;
      auto res = ctx.emit(token, "1");
      if (!res) {
        // Handle or report error
        return res;
      }
    }
    return {};
  }
};

template <typename Mapper = WordCountMapper,
          IsPartitioner Partitioner = HashPartitioner>
inline auto
run_map_phase(std::size_t map_id, const std::filesystem::path &input_file,
              std::size_t num_reducer, const std::filesystem::path &base_dir,
              Mapper mapper = Mapper{}, Partitioner partitioner = Partitioner{},
              std::size_t buffer_size = 64 * 1024)
    -> std::expected<void, StorageError> {

  std::ifstream in(input_file);
  if (!in.is_open()) {
    return std::unexpected(StorageError::IoError);
  }

  MapEmitContext<Partitioner> ctx(map_id, num_reducer, base_dir, partitioner,
                                  buffer_size);

  std::string line;
  std::size_t offset{0};

  while (std::getline(in, line)) {
    auto res = mapper.map(std::to_string(offset), line, ctx);
    if (!res) {
      return res; // Abort early on write failure
    }
    offset += line.length() + 1;
  }

  return ctx.commit();
}

static_assert(IsMapper<WordCountMapper, MapEmitContext<>>,
              "the class is not a mapper");
} // namespace mr
