#pragma once
#include "storage/endian.hpp"
#include <expected>
#include <functional>
#include <optional>
#include <ostream>
#include <string_view>
namespace mr {

/*

 RecordWriter write record to out_, we use serialise key and value
 to write it to buffer
 when we pass std::ofstream
 it has an internal buffer , so when buffer overflow it
 sends data to os, through syscall to kernal pages

 buffer reduces the number of syscall
*/
class RecordWriter {
private:
  std::reference_wrapper<std::ostream> out_;

public:
  enum class ErrorState { KeyOrValueLengthMaxExceeded, IoError };
  explicit RecordWriter(std::ostream &out);

  auto write_record(std::string_view key, std::string_view value)
      -> std::expected<void, ErrorState>;
  auto write_record(std::span<const std::byte> key,
                    std::span<const std::byte> value)
      -> std::expected<void, ErrorState>;
  auto write_packet(const Packet &packet) -> std::expected<void, ErrorState>;

  auto flush() -> std::expected<void, ErrorState>;

  RecordWriter(const RecordWriter &) = delete;
  RecordWriter &operator=(const RecordWriter &) = delete;
  RecordWriter(RecordWriter &&) noexcept = default;
  RecordWriter &operator=(RecordWriter &&) noexcept = default;
};
} // namespace mr