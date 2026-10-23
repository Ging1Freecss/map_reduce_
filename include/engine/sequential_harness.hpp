#pragma once

#include "engine/map_emit_context.hpp"
#include "engine/reduce_emit_context.hpp"
#include <cstddef>
#include <expected>
#include <filesystem>

namespace mr {

/**
 * Executes the complete single-process MapReduce pipeline:
 *
 *   [ Input File ]
 *         │
 *         ▼  1. Map Phase (map_id = 0)
 *    run_map_phase() ──► Produces R intermediate files: mr-0_0.data ... mr-0_(R-1).data
 *         │
 *         ▼  2. Reduce Phase (Iterate r = 0 ... R - 1)
 *    run_reduce_phase() ──► Produces R canonical outputs: mr-out-0 ... mr-out-(R-1)
 */
template <typename Mapper = WordCountMapper,
          typename Reducer = WordCountReducer<>,
          IsPartitioner Partitioner = HashPartitioner>
inline auto run_sequential_map_reduce(
    const std::filesystem::path &input_file,
    const std::filesystem::path &work_dir,
    std::size_t num_reducers,
    Mapper mapper = Mapper{},
    Reducer reducer = Reducer{},
    Partitioner partitioner = Partitioner{},
    std::size_t buffer_size = 64 * 1024)
    -> std::expected<void, StorageError> {

  if (!std::filesystem::exists(input_file)) {
    return std::unexpected(StorageError::IoError);
  }

  std::filesystem::create_directories(work_dir);

  // 1. Run Map Phase (Single map task for the whole input: map_id = 0)
  auto map_res = run_map_phase(
      /*map_id=*/0, input_file, num_reducers, work_dir,
      mapper, partitioner, buffer_size);

  if (!map_res) {
    return map_res;
  }

  // 2. Run Reduce Phase for each partition r = 0 ... num_reducers - 1
  for (std::size_t r = 0; r < num_reducers; ++r) {
    auto reduce_res = run_reduce_phase(
        work_dir, r, /*num_maps=*/1, reducer, buffer_size);

    if (!reduce_res) {
      return reduce_res;
    }
  }

  return {};
}

} // namespace mr
