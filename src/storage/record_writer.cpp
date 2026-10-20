#include "storage/record_writer.hpp"
#include "storage/endian.hpp"
#include <cstdint>
#include <exception>
#include <expected>
#include <limits>
#include <optional>
#include <span>

namespace mr {
RecordWriter::RecordWriter(std::ostream &out) : out_(out) {
  if (!out_.get().good()) {
    throw std::runtime_error{
        "RecordWriter: provided output stream is not in a good state"};
  }
}

auto RecordWriter::write_record(std::string_view key, std::string_view value)
    -> std::expected<void, ErrorState> {
  // converts to std::byte*
  return write_record(std::as_bytes(std::span{key}),
                      std::as_bytes(std::span{value}));
}

auto RecordWriter::write_record(std::span<const std::byte> key,
                                std::span<const std::byte> value)
    -> std::expected<void, ErrorState> {

  std::uint64_t max_length =
      static_cast<std::uint64_t>(std::numeric_limits<std::uint32_t>::max());

  if ((max_length < key.size()) || (max_length < value.size())) {
    return std::unexpected{ErrorState::KeyOrValueLengthMaxExceeded};
  }

  Packet packet_data{};
  packet_data.serialise_data(key, value);
  return write_packet(packet_data);
}

auto RecordWriter::write_packet(const Packet &packet)
    -> std::expected<void, ErrorState> {
  out_.get().write(reinterpret_cast<const char *>(packet.payload().data()),
                   static_cast<std::streamsize>(packet.size()));

  if (!out_.get().good()) {
    return std::unexpected{ErrorState::IoError};
  }
  return {};
}

auto RecordWriter::flush() -> std::expected<void, ErrorState> {
  out_.get().flush();
  if (!out_.get().good()) {
    return std::unexpected{ErrorState::IoError};
  }
  return {};
}
} // namespace mr