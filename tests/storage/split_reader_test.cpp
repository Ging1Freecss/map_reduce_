#include "storage/endian.hpp"
#include "storage/error.hpp"
#include "storage/record_writer.hpp"
#include "storage/split_reader.hpp"
#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace mr {
namespace {

class SplitReaderTest : public ::testing::Test {
protected:
  std::filesystem::path test_dir_;

  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() /
                ("mr_split_reader_test_" + std::to_string(::getpid()));
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(test_dir_, ec);
  }

  // Helper to write a valid record file
  void write_records(const std::filesystem::path &p,
                     const std::vector<std::pair<std::string, std::string>> &recs) {
    std::ofstream out(p, std::ios::binary);
    RecordWriter writer(out);
    for (const auto &[k, v] : recs) {
      auto res = writer.write_record(k, v);
      ASSERT_TRUE(res.has_value());
    }
  }

  // Helper to write raw text files
  void write_text(const std::filesystem::path &p, const std::string &content) {
    std::ofstream out(p, std::ios::binary);
    out.write(content.data(), content.size());
  }
};

// ============================================================================
// generate_binary_splits Tests
// ============================================================================

TEST_F(SplitReaderTest, GenerateSplitsNonExistentFileReturnsIoError) {
  auto res = generate_binary_splits(test_dir_ / "does_not_exist.bin", 64 * 1024);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), StorageError::IoError);
}

TEST_F(SplitReaderTest, GenerateSplitsEmptyFileReturnsZeroSplits) {
  const auto file_path = test_dir_ / "empty.bin";
  {
    std::ofstream empty(file_path, std::ios::binary);
  }

  auto res = generate_binary_splits(file_path, 64 * 1024);
  ASSERT_TRUE(res.has_value());
  EXPECT_TRUE(res->empty());
}

TEST_F(SplitReaderTest, GenerateSplitsSingleRecordSmallerThanTarget) {
  const auto file_path = test_dir_ / "single.bin";
  write_records(file_path, {{"key1", "val1"}});

  // Each record is 4 + 4 + 4 + 4 = 16 bytes
  const auto total_bytes = std::filesystem::file_size(file_path);
  EXPECT_EQ(total_bytes, 16);

  auto res = generate_binary_splits(file_path, 1024);
  ASSERT_TRUE(res.has_value());
  ASSERT_EQ(res->size(), 1);
  EXPECT_EQ((*res)[0].offset, 0);
  EXPECT_EQ((*res)[0].length, total_bytes);
  EXPECT_EQ((*res)[0].file_path, file_path);
}

TEST_F(SplitReaderTest, GenerateSplitsExactBoundaryPartitioning) {
  const auto file_path = test_dir_ / "multi.bin";
  // 4 records of 16 bytes each = 64 bytes total
  write_records(file_path, {
      {"key1", "val1"},
      {"key2", "val2"},
      {"key3", "val3"},
      {"key4", "val4"}
  });

  // Target = 32 bytes (exactly 2 records per split)
  auto res = generate_binary_splits(file_path, 32);
  ASSERT_TRUE(res.has_value());
  ASSERT_EQ(res->size(), 2);

  EXPECT_EQ((*res)[0].offset, 0);
  EXPECT_EQ((*res)[0].length, 32);

  EXPECT_EQ((*res)[1].offset, 32);
  EXPECT_EQ((*res)[1].length, 32);
}

TEST_F(SplitReaderTest, GenerateSplitsGiantRecordExceedsTargetSplitBytes) {
  const auto file_path = test_dir_ / "giant.bin";
  // Small (16B), Giant (4+10+4+100 = 118B), Small (16B)
  std::string giant_val(100, 'x');
  write_records(file_path, {
      {"k1", "v1"},       // 4+2+4+2 = 12 bytes
      {"giant_key", giant_val}, // 4+9+4+100 = 117 bytes
      {"k2", "v2"}        // 12 bytes
  });

  const auto total_bytes = std::filesystem::file_size(file_path);

  // Target split size is only 20 bytes
  auto res = generate_binary_splits(file_path, 20);
  ASSERT_TRUE(res.has_value());

  // Split 1 accumulates 12 + 117 = 129 bytes (exceeds 20, cut at 129)
  // Split 2 takes remainder 12 bytes
  ASSERT_EQ(res->size(), 2);
  EXPECT_EQ((*res)[0].offset, 0);
  EXPECT_EQ((*res)[0].length, 129);

  EXPECT_EQ((*res)[1].offset, 129);
  EXPECT_EQ((*res)[1].length, 12);
  EXPECT_EQ((*res)[0].length + (*res)[1].length, total_bytes);
}

TEST_F(SplitReaderTest, GenerateSplitsZeroTargetDefaultsSafely) {
  const auto file_path = test_dir_ / "default_target.bin";
  write_records(file_path, {{"k", "v"}});

  auto res = generate_binary_splits(file_path, 0);
  ASSERT_TRUE(res.has_value());
  ASSERT_EQ(res->size(), 1);
  EXPECT_EQ((*res)[0].offset, 0);
  EXPECT_EQ((*res)[0].length, std::filesystem::file_size(file_path));
}

TEST_F(SplitReaderTest, GenerateSplitsTruncatedHeaderReturnsError) {
  const auto file_path = test_dir_ / "truncated_hdr.bin";
  write_records(file_path, {{"k1", "v1"}});

  // Append 2 corrupted trailing bytes (partial 4-byte header)
  {
    std::ofstream out(file_path, std::ios::binary | std::ios::app);
    std::uint16_t partial = 0x1234;
    out.write(reinterpret_cast<const char *>(&partial), sizeof(partial));
  }

  auto res = generate_binary_splits(file_path, 1024);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), StorageError::TruncatedHeader);
}

TEST_F(SplitReaderTest, GenerateSplitsTruncatedPayloadReturnsError) {
  const auto file_path = test_dir_ / "truncated_payload.bin";
  {
    std::ofstream out(file_path, std::ios::binary);
    // Key length = 100 bytes (little-endian)
    std::uint32_t klen = to_little_endian_32(100);
    out.write(reinterpret_cast<const char *>(&klen), sizeof(klen));
    // But only write 5 bytes of payload!
    std::string incomplete = "12345";
    out.write(incomplete.data(), incomplete.size());
  }

  auto res = generate_binary_splits(file_path, 1024);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), StorageError::TruncatedPayload);
}

// ============================================================================
// generate_text_splits Tests
// ============================================================================

TEST_F(SplitReaderTest, GenerateTextSplitsNonExistentFileReturnsIoError) {
  auto res = generate_text_splits(test_dir_ / "does_not_exist.txt", 1024);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), StorageError::IoError);
}

TEST_F(SplitReaderTest, GenerateTextSplitsEmptyFileReturnsZeroSplits) {
  const auto file_path = test_dir_ / "empty.txt";
  write_text(file_path, "");

  auto res = generate_text_splits(file_path, 1024);
  ASSERT_TRUE(res.has_value());
  EXPECT_TRUE(res->empty());
}

TEST_F(SplitReaderTest, GenerateTextSplitsSingleLineSmallerThanTarget) {
  const auto file_path = test_dir_ / "single_line.txt";
  const std::string text = "apple banana cherry\n";
  write_text(file_path, text);

  auto res = generate_text_splits(file_path, 1024);
  ASSERT_TRUE(res.has_value());
  ASSERT_EQ(res->size(), 1);
  EXPECT_EQ((*res)[0].offset, 0);
  EXPECT_EQ((*res)[0].length, text.size());
  EXPECT_EQ((*res)[0].file_path, file_path);
}

TEST_F(SplitReaderTest, GenerateTextSplitsAlignsToNewlineBoundary) {
  const auto file_path = test_dir_ / "multiline.txt";
  // Each line is 14 bytes (including '\n'):
  // "Line 1: 12345\n" -> 14 bytes
  // "Line 2: 67890\n" -> 14 bytes
  // "Line 3: abcde\n" -> 14 bytes
  // "Line 4: fghij\n" -> 14 bytes
  // Total: 56 bytes
  const std::string text =
      "Line 1: 12345\n"
      "Line 2: 67890\n"
      "Line 3: abcde\n"
      "Line 4: fghij\n";
  write_text(file_path, text);

  // Target = 20 bytes (falls in the middle of Line 2 at byte 20)
  // Must expand to the end of Line 2 (byte 28)
  auto res = generate_text_splits(file_path, 20);
  ASSERT_TRUE(res.has_value());
  ASSERT_EQ(res->size(), 2);

  // Split 0: Lines 1 & 2 (bytes [0, 28))
  EXPECT_EQ((*res)[0].offset, 0);
  EXPECT_EQ((*res)[0].length, 28);

  // Split 1: Lines 3 & 4 (bytes [28, 56))
  EXPECT_EQ((*res)[1].offset, 28);
  EXPECT_EQ((*res)[1].length, 28);
}

TEST_F(SplitReaderTest, GenerateTextSplitsReconstructionIntegrity) {
  const auto file_path = test_dir_ / "reconstruct.txt";
  const std::string text =
      "first short\n"
      "a much longer second line with several tokens\n"
      "third\n"
      "fourth line here\n"
      "fifth line is the last complete one\n";
  write_text(file_path, text);

  // Use a small target split to force multiple splits
  auto res = generate_text_splits(file_path, 25);
  ASSERT_TRUE(res.has_value());
  ASSERT_GT(res->size(), 1);

  // Read back each slice directly and concatenate
  std::string reconstructed;
  std::ifstream in(file_path, std::ios::binary);
  for (const auto &split : *res) {
    in.seekg(split.offset);
    std::string buf(split.length, '\0');
    in.read(buf.data(), split.length);
    ASSERT_EQ(in.gcount(), split.length);
    reconstructed += buf;
  }

  // Slices must cover the exact file content byte-for-byte
  EXPECT_EQ(reconstructed, text);
}

TEST_F(SplitReaderTest, GenerateTextSplitsFileWithoutTrailingNewline) {
  const auto file_path = test_dir_ / "no_trailing_nl.txt";
  const std::string text = "first line\nsecond line without newline";
  write_text(file_path, text);

  // Target lands in line 2 which has no '\n'
  auto res = generate_text_splits(file_path, 15);
  ASSERT_TRUE(res.has_value());
  ASSERT_FALSE(res->empty());

  // Verify reconstruction covers the entire file
  std::string reconstructed;
  std::ifstream in(file_path, std::ios::binary);
  for (const auto &split : *res) {
    in.seekg(split.offset);
    std::string buf(split.length, '\0');
    in.read(buf.data(), split.length);
    reconstructed += buf;
  }
  EXPECT_EQ(reconstructed, text);
}

TEST_F(SplitReaderTest, GenerateTextSplitsGiantLineExceedingTargetBytes) {
  const auto file_path = test_dir_ / "giant_line.txt";
  const std::string giant_line = "start " + std::string(300, 'x') + " end\n";
  const std::string short_line = "short line\n";
  const std::string text = giant_line + short_line;
  write_text(file_path, text);

  // Target = 50 bytes (giant line is ~311 bytes)
  auto res = generate_text_splits(file_path, 50);
  ASSERT_TRUE(res.has_value());
  ASSERT_GE(res->size(), 2);

  // First split must expand to hold the full giant line
  EXPECT_EQ((*res)[0].offset, 0);
  EXPECT_EQ((*res)[0].length, giant_line.size());

  // Second split starts immediately after giant line
  EXPECT_EQ((*res)[1].offset, giant_line.size());
}

TEST_F(SplitReaderTest, GenerateTextSplitsZeroTargetDefaultsSafely) {
  const auto file_path = test_dir_ / "zero_target.txt";
  const std::string text = "hello world\n";
  write_text(file_path, text);

  // Passing 0 should default to 64MB without dividing by zero or failing
  auto res = generate_text_splits(file_path, 0);
  ASSERT_TRUE(res.has_value());
  ASSERT_EQ(res->size(), 1);
  EXPECT_EQ((*res)[0].offset, 0);
  EXPECT_EQ((*res)[0].length, text.size());
}

// ============================================================================
// SplitReader Class Tests
// ============================================================================

TEST_F(SplitReaderTest, ReaderNonExistentFileReturnsIoError) {
  SplitReader reader(test_dir_ / "missing.bin");
  std::string k, v;
  auto res = reader.next_record(k, v);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), StorageError::IoError);
}

TEST_F(SplitReaderTest, ReaderEmptyFileCleanEOF) {
  const auto file_path = test_dir_ / "empty_reader.bin";
  {
    std::ofstream empty(file_path, std::ios::binary);
  }

  SplitReader reader(file_path);
  std::string k, v;
  auto res = reader.next_record(k, v);
  ASSERT_TRUE(res.has_value());
  EXPECT_FALSE(*res); // Clean EOF
  EXPECT_EQ(reader.bytes_read(), 0);
}

TEST_F(SplitReaderTest, ReaderReadSingleRecord) {
  const auto file_path = test_dir_ / "single_reader.bin";
  write_records(file_path, {{"username", "alice"}});

  SplitReader reader(file_path);
  std::string k, v;
  auto res = reader.next_record(k, v);
  ASSERT_TRUE(res.has_value());
  EXPECT_TRUE(*res);
  EXPECT_EQ(k, "username");
  EXPECT_EQ(v, "alice");

  // Bytes read = 4 + 8 + 4 + 5 = 21
  EXPECT_EQ(reader.bytes_read(), 21);

  // Subsequent call reports clean EOF
  auto eof_res = reader.next_record(k, v);
  ASSERT_TRUE(eof_res.has_value());
  EXPECT_FALSE(*eof_res);
}

TEST_F(SplitReaderTest, ReaderBoundedReadWithMaxBytes) {
  const auto file_path = test_dir_ / "bounded.bin";
  // 4 records of 12 bytes each = 48 bytes total
  write_records(file_path, {
      {"k1", "v1"}, // 12 B
      {"k2", "v2"}, // 12 B
      {"k3", "v3"}, // 12 B
      {"k4", "v4"}  // 12 B
  });

  // Limit reader to 24 bytes (first 2 records)
  SplitReader reader(file_path, 0, 24);
  std::string k, v;

  // Record 1
  auto r1 = reader.next_record(k, v);
  ASSERT_TRUE(r1.has_value() && *r1);
  EXPECT_EQ(k, "k1");

  // Record 2
  auto r2 = reader.next_record(k, v);
  ASSERT_TRUE(r2.has_value() && *r2);
  EXPECT_EQ(k, "k2");

  // Record 3: quota of 24 bytes reached! Must cleanly return false
  auto r3 = reader.next_record(k, v);
  ASSERT_TRUE(r3.has_value());
  EXPECT_FALSE(*r3); // Quota exhausted cleanly
  EXPECT_EQ(reader.bytes_read(), 24);
}

TEST_F(SplitReaderTest, ReaderStartOffsetSeeking) {
  const auto file_path = test_dir_ / "offset_seek.bin";
  write_records(file_path, {
      {"k1", "v1"}, // 12 B [offset 0 .. 12)
      {"k2", "v2"}, // 12 B [offset 12 .. 24)
      {"k3", "v3"}  // 12 B [offset 24 .. 36)
  });

  // Start reader at offset 12 with max_bytes = 12 (should only read k2)
  SplitReader reader(file_path, 12, 12);
  std::string k, v;

  auto r1 = reader.next_record(k, v);
  ASSERT_TRUE(r1.has_value() && *r1);
  EXPECT_EQ(k, "k2");
  EXPECT_EQ(v, "v2");
  EXPECT_EQ(reader.bytes_read(), 12);

  // Next call hits quota
  auto r2 = reader.next_record(k, v);
  ASSERT_TRUE(r2.has_value());
  EXPECT_FALSE(*r2);
}

TEST_F(SplitReaderTest, ReaderCallsAtEofAreIdempotent) {
  const auto file_path = test_dir_ / "idempotent.bin";
  write_records(file_path, {{"foo", "bar"}});

  SplitReader reader(file_path);
  std::string k, v;

  auto r1 = reader.next_record(k, v);
  ASSERT_TRUE(r1.has_value() && *r1);

  // First EOF
  auto eof1 = reader.next_record(k, v);
  ASSERT_TRUE(eof1.has_value());
  EXPECT_FALSE(*eof1);

  // Subsequent EOF calls must stay cleanly false, not return IoError
  auto eof2 = reader.next_record(k, v);
  ASSERT_TRUE(eof2.has_value());
  EXPECT_FALSE(*eof2);

  auto eof3 = reader.next_record(k, v);
  ASSERT_TRUE(eof3.has_value());
  EXPECT_FALSE(*eof3);
}

TEST_F(SplitReaderTest, ReaderTruncatedHeaderReturnsError) {
  const auto file_path = test_dir_ / "reader_trunc_hdr.bin";
  {
    std::ofstream out(file_path, std::ios::binary);
    std::uint16_t partial = 0x42;
    out.write(reinterpret_cast<const char *>(&partial), sizeof(partial));
  }

  SplitReader reader(file_path);
  std::string k, v;
  auto res = reader.next_record(k, v);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), StorageError::TruncatedHeader);
}

TEST_F(SplitReaderTest, ReaderTruncatedPayloadReturnsError) {
  const auto file_path = test_dir_ / "reader_trunc_payload.bin";
  {
    std::ofstream out(file_path, std::ios::binary);
    std::uint32_t klen = to_little_endian_32(50);
    out.write(reinterpret_cast<const char *>(&klen), sizeof(klen));
    std::string partial = "short";
    out.write(partial.data(), partial.size());
  }

  SplitReader reader(file_path);
  std::string k, v;
  auto res = reader.next_record(k, v);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), StorageError::TruncatedPayload);
}

} // namespace
} // namespace mr
