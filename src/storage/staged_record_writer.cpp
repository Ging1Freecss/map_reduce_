#include "storage/staged_record_writer.hpp"
#include <span>
#include <string_view>

namespace mr {

StagedRecordWriter::StagedRecordWriter(const std::filesystem::path &target_path,
                                       std::size_t buffer_size)
    : atomic_file_(target_path, buffer_size), writer_(atomic_file_.stream()) {}

auto StagedRecordWriter::write_record(std::string_view key,
                                      std::string_view value)
    -> std::expected<void, ErrorState> {
  return writer_.write_record(key, value);
}

auto StagedRecordWriter::write_record(std::span<const std::byte> key,
                                      std::span<const std::byte> value)
    -> std::expected<void, ErrorState> {
  return writer_.write_record(key, value);
}

auto StagedRecordWriter::write_record(const Packet &packet)
    -> std::expected<void, ErrorState> {
  return writer_.write_packet(packet);
}

auto StagedRecordWriter::commit() -> std::expected<void, ErrorState> {
  return atomic_file_.commit();
}

auto StagedRecordWriter::target_path() const noexcept
    -> const std::filesystem::path & {
  return atomic_file_.target_path();
}

auto StagedRecordWriter::is_committed() const noexcept -> bool {
  return atomic_file_.is_committed();
}

} // namespace mr