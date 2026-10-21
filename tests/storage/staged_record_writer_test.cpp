#include "storage/staged_record_writer.hpp"
#include "storage/record_reader.hpp"
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace mr {
namespace {

class StagedRecordWriterTest : public ::testing::Test {
protected:
  std::filesystem::path test_dir_;

  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() /
                ("mr_staged_writer_test_" + std::to_string(::getpid()));
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(test_dir_, ec);
  }
};

} // namespace

TEST_F(StagedRecordWriterTest, SuccessfulWriteAndCommit) {
  const auto target = test_dir_ / "output.bin";

  {
    StagedRecordWriter writer{target};
    EXPECT_EQ(writer.target_path(), target);
    EXPECT_FALSE(writer.is_committed());

    auto r1 = writer.write_record("user_1", "click");
    ASSERT_TRUE(r1.has_value());

    auto r2 = writer.write_record("user_2", "purchase");
    ASSERT_TRUE(r2.has_value());

    auto commit_res = writer.commit();
    ASSERT_TRUE(commit_res.has_value());
    EXPECT_TRUE(writer.is_committed());
  }

  // Verify target exists
  ASSERT_TRUE(std::filesystem::exists(target));

  // Read back records via RecordReader
  std::ifstream in{target, std::ios::binary};
  RecordReader reader{in};

  std::string k, v;
  auto res1 = reader.next_record(k, v);
  ASSERT_TRUE(res1.has_value() && *res1);
  EXPECT_EQ(k, "user_1");
  EXPECT_EQ(v, "click");

  auto res2 = reader.next_record(k, v);
  ASSERT_TRUE(res2.has_value() && *res2);
  EXPECT_EQ(k, "user_2");
  EXPECT_EQ(v, "purchase");

  auto res3 = reader.next_record(k, v);
  ASSERT_TRUE(res3.has_value() && !*res3); // Clean EOF
}

TEST_F(StagedRecordWriterTest, RollbackOnScopeExitWithoutCommit) {
  const auto target = test_dir_ / "uncommitted.bin";

  {
    StagedRecordWriter writer{target};
    auto res = writer.write_record("abandoned_key", "abandoned_val");
    ASSERT_TRUE(res.has_value());
    // Deliberately do not call commit()
  }

  // Target must never have been created
  EXPECT_FALSE(std::filesystem::exists(target));
}

TEST_F(StagedRecordWriterTest, RollbackOnException) {
  const auto target = test_dir_ / "exception.bin";

  try {
    StagedRecordWriter writer{target};
    auto res = writer.write_record("crash_key", "crash_val");
    ASSERT_TRUE(res.has_value());
    throw std::runtime_error("simulated crash during write");
  } catch (const std::runtime_error &) {
    // caught
  }

  EXPECT_FALSE(std::filesystem::exists(target));
}

TEST_F(StagedRecordWriterTest, WriteBinaryAndPacketOverloads) {
  const auto target = test_dir_ / "binary.bin";

  {
    StagedRecordWriter writer{target};

    // 1. Write via std::span<const std::byte>
    std::vector<std::byte> raw_key{std::byte{0x00}, std::byte{0xFF}, std::byte{0x7F}};
    std::vector<std::byte> raw_val{std::byte{0xAA}, std::byte{0xBB}};
    auto r1 = writer.write_record(raw_key, raw_val);
    ASSERT_TRUE(r1.has_value());

    // 2. Write via Packet directly
    Packet packet;
    packet.serialise_data(
        std::as_bytes(std::span{std::string_view{"packet_key"}}),
        std::as_bytes(std::span{std::string_view{"packet_val"}}));
    auto r2 = writer.write_record(packet);
    ASSERT_TRUE(r2.has_value());

    ASSERT_TRUE(writer.commit().has_value());
  }

  // Read back
  std::ifstream in{target, std::ios::binary};
  RecordReader reader{in};

  std::string k, v;
  auto res1 = reader.next_record(k, v);
  ASSERT_TRUE(res1.has_value() && *res1);
  EXPECT_EQ(k.size(), 3);
  EXPECT_EQ(static_cast<uint8_t>(k[0]), 0x00);
  EXPECT_EQ(static_cast<uint8_t>(k[1]), 0xFF);
  EXPECT_EQ(static_cast<uint8_t>(k[2]), 0x7F);

  auto res2 = reader.next_record(k, v);
  ASSERT_TRUE(res2.has_value() && *res2);
  EXPECT_EQ(k, "packet_key");
  EXPECT_EQ(v, "packet_val");

  auto res3 = reader.next_record(k, v);
  ASSERT_TRUE(res3.has_value() && !*res3);
}

TEST_F(StagedRecordWriterTest, AtomicOverwriteOfExistingTarget) {
  const auto target = test_dir_ / "overwrite.bin";

  // Pre-create original file
  {
    std::ofstream out{target};
    out << "ORIGINAL_DATA";
  }

  {
    StagedRecordWriter writer{target};
    auto r = writer.write_record("new_key", "new_val");
    ASSERT_TRUE(r.has_value());

    // Target still holds old content before commit
    std::ifstream check{target};
    std::string line;
    std::getline(check, line);
    EXPECT_EQ(line, "ORIGINAL_DATA");

    ASSERT_TRUE(writer.commit().has_value());
  }

  // Target replaced atomically
  std::ifstream in{target, std::ios::binary};
  RecordReader reader{in};
  std::string k, v;
  auto res = reader.next_record(k, v);
  ASSERT_TRUE(res.has_value() && *res);
  EXPECT_EQ(k, "new_key");
  EXPECT_EQ(v, "new_val");
}

TEST_F(StagedRecordWriterTest, CommitIsIdempotent) {
  const auto target = test_dir_ / "idempotent.bin";

  StagedRecordWriter writer{target};
  ASSERT_TRUE(writer.write_record("key", "val").has_value());

  auto c1 = writer.commit();
  EXPECT_TRUE(c1.has_value());

  auto c2 = writer.commit();
  EXPECT_TRUE(c2.has_value());
}

} // namespace mr
