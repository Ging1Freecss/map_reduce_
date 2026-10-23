#include "storage/record_reader.hpp"
#include "storage/endian.hpp"
#include <cstddef>
#include <cstdint>
#include <ios>
#include <istream>
#include <stdexcept>

namespace mr {
RecordReader::RecordReader(std::istream &in) : in_(in) {
  if (!in_.get().good()) {
    throw std::runtime_error("steam is invalid ");
  }
}

auto RecordReader::next_record(std::string &out_key, std::string &out_value)
    -> std::expected<bool, ErrorState> {

  std::uint32_t endian_key_length{0};
  std::uint32_t endian_value_length{0};

  // read key length
  in_.get().read(reinterpret_cast<char *>(&endian_key_length),
                 static_cast<std::streamsize>(sizeof(std::uint32_t)));
  if (in_.get().bad()) {
    return std::unexpected(ErrorState::IoError);
  }

  if (in_.get().gcount() == 0) {
    return false; // EOF, only after checking first if key exist
  }

  if (in_.get().gcount() > 0 &&
      in_.get().gcount() !=
          static_cast<std::streamsize>(sizeof(std::uint32_t))) {
    return std::unexpected(ErrorState::TruncatedHeader);
  }

  std::size_t key_length{from_little_endian_32(endian_key_length)};

  // read key data
  /*
    in both out_key and out_value we resize to 0 when length is 0 even
    when not writing anything to it, as the user depend on if 
    out_key is empty or not, if we dont resize it user will 
    push the previous key value 
  */
  out_key.resize(key_length);
  if (key_length > 0) {

    in_.get().read(out_key.data(), key_length);
    if (in_.get().bad()) {
      return std::unexpected(ErrorState::IoError);
    }
    if (in_.get().gcount() != static_cast<std::streamsize>(key_length)) {
      return std::unexpected(ErrorState::TruncatedPayload);
    }
  }
  // read value_length
  in_.get().read(reinterpret_cast<char *>(&endian_value_length),
                 static_cast<std::streamsize>(sizeof(endian_value_length)));

  if (in_.get().bad()) {
    return std::unexpected(ErrorState::IoError);
  }

  if (in_.get().gcount() !=
      static_cast<std::streamsize>(sizeof(std::uint32_t))) {
    return std::unexpected(ErrorState::TruncatedHeader);
  }

  std::uint32_t value_length = from_little_endian_32(endian_value_length);

  // read value data
  out_value.resize(value_length);
  if (value_length > 0) {
    in_.get().read(out_value.data(), value_length);
    if (in_.get().bad()) {
      return std::unexpected(ErrorState::IoError);
    }
    if (in_.get().gcount() != static_cast<std::streamsize>(value_length)) {
      return std::unexpected(ErrorState::TruncatedPayload);
    }
  }
  return true;
}

} // namespace mr
