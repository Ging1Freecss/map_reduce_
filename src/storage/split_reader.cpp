#include "storage/split_reader.hpp"
#include "storage/endian.hpp"
#include "storage/error.hpp"
#include <cstdint>
#include <expected>
#include <fstream>
#include <ios>
#include <iosfwd>
#include <vector>

namespace mr {

auto generate_text_splits(const std::filesystem::path &file_path,
                          uint64_t target_split_bytes)
    -> std::expected<std::vector<InputSplit>, StorageError> {
  std::vector<InputSplit> splits;

  if (target_split_bytes == 0) {
    target_split_bytes = 64 * 1024 * 1024;
  }

  std::ifstream file(file_path, std::ios::binary);

  if (!file.is_open()) {
    return std::unexpected(StorageError::IoError);
  }

  file.seekg(0, std::ios::end);
  const std::streamsize end_pos = file.tellg();

  // check  error on seek
  if (end_pos == std::streampos(-1)) {
    return std::unexpected(StorageError::IoError);
  }

  // 0-byte file
  if (end_pos == std::streampos(0)) {
    return splits;
  }

  const std::uint64_t total_file_size = static_cast<std::uint64_t>(end_pos);

  file.seekg(0, std::ios::beg);

  std::uint64_t curr{0};

  /*
    goal is insert range [curr,x) where x = target + y, y >=0 such it is valid
   means at x, char = "\n" or it is the last char
    */
  while (curr < total_file_size) {

    std::uint64_t target_end = curr + target_split_bytes;

    if (target_end >= total_file_size) {
      // [curr,total_file_size) range, 0 indexing
      // total_file_size is excluded
      // index goes from 0 to total_file_size - 1
      splits.push_back({file_path, curr, total_file_size - curr});
      break;
    }

    file.seekg(target_end);
    char ch{};

    while (file.get(ch)) {
      /*
        unlike map phase code where i use
        std::isspace(ch) to break

        std::isspace returns true for:
        ' ' (space)
        '\t' (tab)
        '\n' (newline)
        '\r' (carriage return)

        it is used for tokenize the data
        here we send raw data to map worker
        separated with '\n'

        in map phase we read file using std::getline
        which expect a line end with '\n' or EOF
      */
      if (ch == '\n') {
        break;
      }
    }

    std::uint64_t end{total_file_size};

    /*
      the above loop, work until it finds '\n'
      if end ch in the file does not have '\n'
      file read the last ch trigger eof
      file find no bytes for ch, trigger fail

     Clear any EOF/fail flags so subsequent operations succeed:
     file.clear()
    */
    /*
      if lase char = \n then only eof
      else eof and fail , but we only need eof to work with
    */
    if (file.eof()) {
      file.clear();
      end = total_file_size;
    } else {

      const auto pos = file.tellg();
      if (pos == std::streampos(-1)) {
        return std::unexpected(StorageError::IoError);
      }
      end = static_cast<std::uint64_t>(pos);
    }

    uint64_t split_length = end - curr;

    splits.push_back({file_path, curr, split_length});
    curr = end;
  }

  return splits;
}

/*
    core logic is simple, we have curr and last cursor
    when range -> [curr,last) greater than target
    we insert that range into splits vector and keep on doing
    this till EOF, it may happen last end of file range
    is not >= target thats fine, we just insert it into
    the end
*/
auto generate_binary_splits(const std::filesystem::path &file_path,
                            std::uint64_t target_split_bytes)
    -> std::expected<std::vector<InputSplit>, StorageError> {

  std::vector<InputSplit> splits;

  // minimum valid split size, avoid divide by 0
  if (target_split_bytes == 0) {
    target_split_bytes = 64 * 1024 * 1024;
  }

  std::ifstream file(file_path, std::ios::binary);

  if (!file.is_open()) {
    return std::unexpected(StorageError::IoError);
  }

  /*
    seekg(length,std::ios argument) and tellg() work together
    seekg(length,std::ios argument) -> set the cursor base on argument
    length is +ve moves to right
    length is -ve moves to left

    std::ios::end -> cursor set at end
    std::ios::beg -> cursor set at beginning
    std::ios::cur -> cursor set at current internal pointer

    seekg(pos) -> moves to absolute point
    pos and length count raw bytes
    tellg() gives the position of current cursor from the beginning
  */

  file.seekg(0, std::ios::end);
  const std::streamsize end_pos = file.tellg();

  // check  error on seek
  if (end_pos == std::streampos(-1)) {
    return std::unexpected(StorageError::IoError);
  }

  // 0-byte file
  if (end_pos == std::streampos(0)) {
    return splits;
  }

  const std::uint64_t total_file_size = static_cast<std::uint64_t>(end_pos);

  file.seekg(0, std::ios::beg);

  std::uint64_t current_split_start{0};
  std::uint64_t last_valid_record_end{0};

  while (file.good()) {

    const std::streampos rec_start_pos = file.tellg();

    if (rec_start_pos == std::streampos(-1)) {
      // error at tellg
      return std::unexpected(StorageError::IoError);
    }

    const std::uint64_t rec_start = static_cast<std::uint64_t>(rec_start_pos);

    if (rec_start >= total_file_size) {
      break; // EOF
    }

    // read key length
    std::uint32_t raw_key_len{0};
    file.read(reinterpret_cast<char *>(&raw_key_len), sizeof(raw_key_len));

    if (file.bad()) {
      return std::unexpected(StorageError::IoError);
    }

    if (file.gcount() != sizeof(raw_key_len)) {
      return std::unexpected(
          StorageError::TruncatedHeader); // truncated header at EOF
    }

    const std::uint32_t key_len = from_little_endian_32(raw_key_len);

    // boundary error check, if data is truncated
    if (rec_start + sizeof(raw_key_len) + key_len > total_file_size) {
      return std::unexpected(StorageError::TruncatedPayload);
    }
    // skip key payload
    file.seekg(key_len, std::ios::cur);
    if (!file.good()) {
      return std::unexpected(StorageError::TruncatedPayload);
    }

    // read value length
    std::uint32_t raw_value_length{0};
    file.read(reinterpret_cast<char *>(&raw_value_length),
              sizeof(raw_value_length));

    if (file.bad()) {
      return std::unexpected(StorageError::IoError);
    }

    if (file.gcount() != sizeof(raw_value_length)) {
      return std::unexpected(StorageError::TruncatedHeader);
    }

    const std::uint32_t val_len = from_little_endian_32(raw_value_length);

    // skip value payload
    const std::streampos val_payload_start = file.tellg();
    if (val_payload_start == std::streampos(-1)) {
      return std::unexpected(StorageError::IoError);
    }

    if (static_cast<std::uint64_t>(val_payload_start) +
            static_cast<std::uint64_t>(val_len) >
        total_file_size) {
      return std::unexpected(StorageError::TruncatedPayload);
    }

    file.seekg(val_len, std::ios::cur);
    if (!file.good()) {
      return std::unexpected(StorageError::TruncatedPayload);
    }

    // both value and key are verified on disk
    const std::streampos rec_end_pos = file.tellg();
    if (rec_end_pos == std::streampos(-1)) {
      return std::unexpected(StorageError::IoError);
    }

    last_valid_record_end = static_cast<std::uint64_t>(rec_end_pos);

    // check if target split size reached
    const std::uint64_t acc = last_valid_record_end - current_split_start;

    /*
        target bytes reached so range [curr,last)
        should be >= target

        so curr = last, for next range of bytes
    */
    if (acc >= target_split_bytes) {
      splits.push_back(InputSplit{
          .file_path = file_path,
          .offset = current_split_start,
          .length = acc,
      });

      current_split_start = last_valid_record_end;
    }
  }

  // verify if we reached EOF
  if (last_valid_record_end != total_file_size) {
    return std::unexpected(StorageError::TruncatedPayload);
  }

  if (last_valid_record_end > current_split_start) {
    splits.push_back(InputSplit{
        .file_path = file_path,
        .offset = current_split_start,
        .length = last_valid_record_end - current_split_start,
    });
  }

  return splits;
}

SplitReader::SplitReader(const std::filesystem::path &file_path,
                         std::uint64_t start_offset,
                         std::optional<std::uint64_t> max_bytes)
    : file_(file_path, std::ios::binary), max_bytes_(max_bytes) {
  if (file_.is_open() && start_offset > 0) {
    file_.seekg(static_cast<std::streamoff>(start_offset), std::ios::beg);
  }
}

/*
    file_.bad instead of !file_.good
    so we can separate
    fatal error in disk i/o => file_.bad
    and   0 < file_.gcount() < N => Truncated record
*/
auto SplitReader::next_record(std::string &out_key, std::string &out_val)
    -> std::expected<bool, ErrorState> {
  // target completed
  if (max_bytes_.has_value() && bytes_read_ >= max_bytes_.value()) {
    return false;
  }

  if (!file_.is_open() || file_.bad()) {
    return std::unexpected<ErrorState>(ErrorState::IoError);
  }

  if (file_.eof()) {
    return false;
  }

  // key and key_payload
  std::uint32_t raw_key = 0;
  file_.read(reinterpret_cast<char *>(&raw_key), sizeof(raw_key));
  if (file_.bad())
    return std::unexpected<ErrorState>(ErrorState::IoError);

  if (file_.gcount() == 0)
    return false; // Clean EOF at record boundary

  if (file_.gcount() != sizeof(raw_key))
    return std::unexpected<ErrorState>(ErrorState::TruncatedHeader);

  const std::uint32_t key_length = from_little_endian_32(raw_key);
  out_key.resize(key_length);

  //  Key Payload
  if (key_length > 0) {
    file_.read(out_key.data(), key_length);

    if (file_.bad())
      return std::unexpected<ErrorState>(ErrorState::IoError);

    if (static_cast<std::uint32_t>(file_.gcount()) != key_length)
      return std::unexpected<ErrorState>(ErrorState::TruncatedPayload);
  }

  // Value Length Header
  std::uint32_t raw_value = 0;

  file_.read(reinterpret_cast<char *>(&raw_value), sizeof(raw_value));

  if (file_.bad())
    return std::unexpected<ErrorState>(ErrorState::IoError);

  if (file_.gcount() != sizeof(raw_value))
    return std::unexpected<ErrorState>(ErrorState::TruncatedHeader);

  const std::uint32_t value_length = from_little_endian_32(raw_value);
  out_val.resize(value_length);

  // Value Payload
  if (value_length > 0) {
    file_.read(out_val.data(), value_length);

    if (file_.bad())
      return std::unexpected<ErrorState>(ErrorState::IoError);

    if (static_cast<std::uint32_t>(file_.gcount()) != value_length)
      return std::unexpected<ErrorState>(ErrorState::TruncatedPayload);
  }

  bytes_read_ +=
      sizeof(raw_key) + key_length + sizeof(raw_value) + value_length;
  return true;
}

std::uint64_t SplitReader::bytes_read() const noexcept { return bytes_read_; }
} // namespace mr