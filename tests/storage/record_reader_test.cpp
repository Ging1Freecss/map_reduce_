#include "storage/endian.hpp"
#include "storage/record_reader.hpp"
#include "storage/record_writer.hpp"
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace mr {
namespace {

auto to_bytes(std::string_view sv) noexcept -> std::span<const std::byte> {
  return std::as_bytes(std::span{sv.data(), sv.size()});
}

} // namespace

// ============================================================================
// RecordReader Unit Tests
// ============================================================================

TEST(RecordReaderTest, ConstructorThrowsOnBadStream) {
  std::istringstream iss;
  iss.setstate(std::ios_base::badbit);
  EXPECT_THROW((RecordReader{iss}), std::runtime_error);
}

TEST(RecordReaderTest, CleanEOFOnEmptyStream) {
  std::istringstream iss{""};
  RecordReader reader{iss};

  std::string key, value;
  auto res = reader.next_record(key, value);

  ASSERT_TRUE(res.has_value());
  EXPECT_FALSE(*res); // false means Clean EOF
}

TEST(RecordReaderTest, ReadSingleRecord) {
  std::stringstream ss;
  RecordWriter writer{ss};
  auto write_res = writer.write_record("user_name", "alice");
  ASSERT_TRUE(write_res.has_value());

  RecordReader reader{ss};
  std::string key, value;

  auto read_res = reader.next_record(key, value);
  ASSERT_TRUE(read_res.has_value());
  EXPECT_TRUE(*read_res);
  EXPECT_EQ(key, "user_name");
  EXPECT_EQ(value, "alice");

  // Subsequent read must report clean EOF
  auto eof_res = reader.next_record(key, value);
  ASSERT_TRUE(eof_res.has_value());
  EXPECT_FALSE(*eof_res);
}

TEST(RecordReaderTest, ReadMultipleRecordsSequential) {
  std::stringstream ss;
  RecordWriter writer{ss};

  const std::vector<std::pair<std::string, std::string>> records = {
      {"key_1", "value_1"},
      {"apple", "1"},
      {"banana", "2"},
      {"orange", "3"},
      {"long_key_name_with_extra_characters", "long_value_data_chunk_for_testing"}};

  for (const auto &[k, v] : records) {
    ASSERT_TRUE(writer.write_record(k, v).has_value());
  }

  RecordReader reader{ss};
  std::string key, value;

  for (std::size_t i = 0; i < records.size(); ++i) {
    auto res = reader.next_record(key, value);
    ASSERT_TRUE(res.has_value()) << "Failed at index " << i;
    EXPECT_TRUE(*res);
    EXPECT_EQ(key, records[i].first);
    EXPECT_EQ(value, records[i].second);
  }

  // End of file
  auto eof_res = reader.next_record(key, value);
  ASSERT_TRUE(eof_res.has_value());
  EXPECT_FALSE(*eof_res);
}

TEST(RecordReaderTest, BufferReuseAcrossRecordsWithEmptyKeysAndValues) {
  std::stringstream ss;
  RecordWriter writer{ss};

  // Record 1: long key & long value
  ASSERT_TRUE(writer.write_record("long_key_string", "long_value_string").has_value());
  // Record 2: empty key, non-empty value
  ASSERT_TRUE(writer.write_record("", "global_sum").has_value());
  // Record 3: non-empty key, empty value
  ASSERT_TRUE(writer.write_record("unique_set_key", "").has_value());
  // Record 4: both empty
  ASSERT_TRUE(writer.write_record("", "").has_value());

  RecordReader reader{ss};
  std::string key, value;

  // 1. First record
  auto r1 = reader.next_record(key, value);
  ASSERT_TRUE(r1.has_value() && *r1);
  EXPECT_EQ(key, "long_key_string");
  EXPECT_EQ(value, "long_value_string");

  // 2. Second record (empty key must NOT retain "long_key_string")
  auto r2 = reader.next_record(key, value);
  ASSERT_TRUE(r2.has_value() && *r2);
  EXPECT_TRUE(key.empty());
  EXPECT_EQ(value, "global_sum");

  // 3. Third record (empty value must NOT retain "global_sum")
  auto r3 = reader.next_record(key, value);
  ASSERT_TRUE(r3.has_value() && *r3);
  EXPECT_EQ(key, "unique_set_key");
  EXPECT_TRUE(value.empty());

  // 4. Fourth record (both empty)
  auto r4 = reader.next_record(key, value);
  ASSERT_TRUE(r4.has_value() && *r4);
  EXPECT_TRUE(key.empty());
  EXPECT_TRUE(value.empty());

  // EOF
  auto eof_res = reader.next_record(key, value);
  ASSERT_TRUE(eof_res.has_value() && !*eof_res);
}

TEST(RecordReaderTest, BinaryPayloadsWithNullBytes) {
  std::stringstream ss;
  RecordWriter writer{ss};

  const std::string bin_key("prefix\0mid\0suffix", 17);
  const std::string bin_val("val\0\xFF\xFE\0end", 11);

  ASSERT_TRUE(writer.write_record(bin_key, bin_val).has_value());

  RecordReader reader{ss};
  std::string key, value;

  auto res = reader.next_record(key, value);
  ASSERT_TRUE(res.has_value() && *res);
  EXPECT_EQ(key.size(), 17U);
  EXPECT_EQ(value.size(), 11U);
  EXPECT_EQ(key, bin_key);
  EXPECT_EQ(value, bin_val);
}

TEST(RecordReaderTest, DetectTruncatedKeyHeader) {
  // Feed 1, 2, or 3 bytes (incomplete 4-byte key length header)
  for (std::size_t len = 1; len < sizeof(std::uint32_t); ++len) {
    std::string partial_header(len, '\x01');
    std::istringstream iss{partial_header};
    RecordReader reader{iss};

    std::string key, value;
    auto res = reader.next_record(key, value);
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), RecordReader::ErrorState::TruncatedHeader);
  }
}

TEST(RecordReaderTest, DetectTruncatedKeyPayload) {
  // Key header says length 10, but only 4 bytes of data are present
  std::string corrupted;
  std::uint32_t key_len = to_little_endian_32(10);
  corrupted.append(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
  corrupted.append("data"); // only 4 bytes, needed 10

  std::istringstream iss{corrupted};
  RecordReader reader{iss};

  std::string key, value;
  auto res = reader.next_record(key, value);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), RecordReader::ErrorState::TruncatedPayload);
}

TEST(RecordReaderTest, DetectTruncatedValueHeader) {
  // Valid key, but only 2 bytes for value length header
  std::string corrupted;
  std::uint32_t key_len = to_little_endian_32(3);
  corrupted.append(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
  corrupted.append("cat");
  corrupted.append("\x01\x00"); // only 2 bytes of value length

  std::istringstream iss{corrupted};
  RecordReader reader{iss};

  std::string key, value;
  auto res = reader.next_record(key, value);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), RecordReader::ErrorState::TruncatedHeader);
}

TEST(RecordReaderTest, DetectTruncatedValuePayload) {
  // Valid key, valid value header saying 8 bytes, but only 3 bytes of value
  std::string corrupted;
  std::uint32_t key_len = to_little_endian_32(3);
  corrupted.append(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
  corrupted.append("cat");

  std::uint32_t val_len = to_little_endian_32(8);
  corrupted.append(reinterpret_cast<const char*>(&val_len), sizeof(val_len));
  corrupted.append("123"); // only 3 bytes, needed 8

  std::istringstream iss{corrupted};
  RecordReader reader{iss};

  std::string key, value;
  auto res = reader.next_record(key, value);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), RecordReader::ErrorState::TruncatedPayload);
}

TEST(RecordReaderTest, MoveSemantics) {
  std::stringstream ss;
  RecordWriter writer{ss};
  ASSERT_TRUE(writer.write_record("k", "v").has_value());

  RecordReader reader1{ss};
  RecordReader reader2{std::move(reader1)};

  std::string key, value;
  auto res = reader2.next_record(key, value);
  ASSERT_TRUE(res.has_value() && *res);
  EXPECT_EQ(key, "k");
  EXPECT_EQ(value, "v");
}

} // namespace mr
