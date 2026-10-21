#pragma once

#include "storage/atomic_file.hpp"
#include "storage/error.hpp"
#include "storage/record_writer.hpp"
#include <expected>
#include <filesystem>
#include <span>
#include <string_view>

namespace mr {
class StagedRecordWriter {
public:
  using ErrorState = StorageError;

  explicit StagedRecordWriter(const std::filesystem::path &target_path,
                              std::size_t buffer_size = 64 * 1024);
  ~StagedRecordWriter() = default;

  StagedRecordWriter(const StagedRecordWriter &) = delete;
  StagedRecordWriter &operator=(const StagedRecordWriter &) = delete;
  StagedRecordWriter(StagedRecordWriter &&) = delete;
  StagedRecordWriter &operator=(StagedRecordWriter &&) = delete;

  // Record writing
  auto write_record(std::string_view key, std::string_view value)
      -> std::expected<void, ErrorState>;
  auto write_record(std::span<const std::byte> key,
                    std::span<const std::byte> value)
      -> std::expected<void, ErrorState>;
  auto write_record(const Packet &packet) -> std::expected<void, ErrorState>;

  // Flushes writer and commits atomic file
  auto commit() -> std::expected<void, ErrorState>;

  [[nodiscard]] auto target_path() const noexcept
      -> const std::filesystem::path &;
  [[nodiscard]] auto is_committed() const noexcept -> bool;

private:
  AtomicFile atomic_file_; // MUST be first
  RecordWriter writer_;    // MUST be second
};

} // namespace mr