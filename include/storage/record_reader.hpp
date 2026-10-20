#pragma once

#include "storage/endian.hpp"
#include <expected>
#include <functional>
#include <istream>
#include <string>

namespace mr {

/*
    deserialise key and value pair one at a time using next_record()
    we have call next_record() repeatedly to get next pair till EOF
*/
class RecordReader {
private:
  std::reference_wrapper<std::istream> in_;

public:
  enum class ErrorState {
    IoError,         // stream encountered an error
    TruncatedHeader, // Stream ran out while reading 4-byte length prefix
    TruncatedPayload // stream ran out before reading the promised key_len or
                     // val_len
  };

  RecordReader(std::istream &in);

  auto next_record(std::string &out_key, std::string &out_value)
      -> std::expected<bool, ErrorState>;

  RecordReader(const RecordReader &) = delete;
  RecordReader &operator=(const RecordReader &) = delete;
  RecordReader(RecordReader &&) = default;
  RecordReader &operator=(RecordReader &&) = default;
};
} // namespace mr