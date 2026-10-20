#include "storage/atomic_file.hpp"
#include "storage/record_reader.hpp"
#include "storage/record_writer.hpp"
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

namespace mr {
namespace {

class AtomicFileTest : public ::testing::Test {
protected:
  std::filesystem::path test_dir_;

  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() /
                ("mr_atomic_test_" + std::to_string(::getpid()));
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(test_dir_, ec);
  }
};

} // namespace

TEST_F(AtomicFileTest, SuccessfulCommitReplacesTarget) {
  const auto target = test_dir_ / "output.bin";
  std::filesystem::path staging;

  {
    AtomicFile file{target};
    staging = file.staging_path();

    EXPECT_FALSE(file.is_committed());
    EXPECT_TRUE(std::filesystem::exists(staging));
    EXPECT_FALSE(std::filesystem::exists(target));

    file.stream() << "hello atomic world";

    auto res = file.commit();
    ASSERT_TRUE(res.has_value());
    EXPECT_TRUE(file.is_committed());
  }

  // Staging must be removed, target must exist
  EXPECT_FALSE(std::filesystem::exists(staging));
  EXPECT_TRUE(std::filesystem::exists(target));

  // Verify file content
  std::ifstream in{target};
  std::string content;
  std::getline(in, content);
  EXPECT_EQ(content, "hello atomic world");
}

TEST_F(AtomicFileTest, RollbackOnScopeExitWithoutCommit) {
  const auto target = test_dir_ / "uncommitted.bin";
  std::filesystem::path staging;

  {
    AtomicFile file{target};
    staging = file.staging_path();

    EXPECT_TRUE(std::filesystem::exists(staging));
    file.stream() << "this should be rolled back";
    // Intentionally DO NOT call commit()
  }

  // Destructor must have deleted staging, target must never have been created
  EXPECT_FALSE(std::filesystem::exists(staging));
  EXPECT_FALSE(std::filesystem::exists(target));
}

TEST_F(AtomicFileTest, RollbackOnException) {
  const auto target = test_dir_ / "exception.bin";
  std::filesystem::path staging;

  try {
    AtomicFile file{target};
    staging = file.staging_path();

    EXPECT_TRUE(std::filesystem::exists(staging));
    file.stream() << "partial data before crash";

    throw std::runtime_error("simulated worker failure");
  } catch (const std::runtime_error &) {
    // Exception caught
  }

  // Staging must be purged after exception unwinding
  EXPECT_FALSE(std::filesystem::exists(staging));
  EXPECT_FALSE(std::filesystem::exists(target));
}

TEST_F(AtomicFileTest, SameDirectoryStagingInvariant) {
  const auto nested_target = test_dir_ / "subfolder" / "partition_0.bin";
  AtomicFile file{nested_target};

  // Staging file must reside in the exact same parent folder to prevent EXDEV
  EXPECT_EQ(file.staging_path().parent_path(), file.target_path().parent_path());
}

TEST_F(AtomicFileTest, AtomicOverwriteOfExistingTarget) {
  const auto target = test_dir_ / "existing.bin";

  // 1. Create original target file
  {
    std::ofstream out{target};
    out << "OLD_VERSION_DATA";
  }
  ASSERT_TRUE(std::filesystem::exists(target));

  // 2. Begin atomic overwrite
  {
    AtomicFile file{target};
    file.stream() << "NEW_VERSION_DATA";

    // Before commit, old data is still untouched in target
    std::ifstream check_old{target};
    std::string current;
    std::getline(check_old, current);
    EXPECT_EQ(current, "OLD_VERSION_DATA");

    ASSERT_TRUE(file.commit().has_value());
  }

  // 3. After commit, new data replaced old data atomically
  std::ifstream check_new{target};
  std::string updated;
  std::getline(check_new, updated);
  EXPECT_EQ(updated, "NEW_VERSION_DATA");
}

TEST_F(AtomicFileTest, MoveConstructorTransfersOwnership) {
  const auto target = test_dir_ / "moved.bin";
  std::filesystem::path staging;

  {
    AtomicFile file1{target};
    staging = file1.staging_path();

    // Move construct file2 from file1
    AtomicFile file2{std::move(file1)};

    // file1's destructor should not delete staging file
    EXPECT_TRUE(std::filesystem::exists(staging));

    file2.stream() << "moved stream content";
    ASSERT_TRUE(file2.commit().has_value());
  }

  EXPECT_FALSE(std::filesystem::exists(staging));
  EXPECT_TRUE(std::filesystem::exists(target));

  std::ifstream in{target};
  std::string content;
  std::getline(in, content);
  EXPECT_EQ(content, "moved stream content");
}

TEST_F(AtomicFileTest, MoveAssignmentCleansUpExistingStaging) {
  const auto target1 = test_dir_ / "first.bin";
  const auto target2 = test_dir_ / "second.bin";

  std::filesystem::path staging1;
  std::filesystem::path staging2;

  {
    AtomicFile file1{target1};
    AtomicFile file2{target2};

    staging1 = file1.staging_path();
    staging2 = file2.staging_path();

    EXPECT_TRUE(std::filesystem::exists(staging1));
    EXPECT_TRUE(std::filesystem::exists(staging2));

    // Move assign file2 into file1: file1's old staging1 should be deleted!
    file1 = std::move(file2);

    EXPECT_FALSE(std::filesystem::exists(staging1));
    EXPECT_TRUE(std::filesystem::exists(staging2));

    file1.stream() << "second content";
    ASSERT_TRUE(file1.commit().has_value());
  }

  EXPECT_FALSE(std::filesystem::exists(staging2));
  EXPECT_TRUE(std::filesystem::exists(target2));
}

TEST_F(AtomicFileTest, MultipleCommitsAreIdempotent) {
  const auto target = test_dir_ / "idempotent.bin";
  AtomicFile file{target};

  file.stream() << "test";
  auto r1 = file.commit();
  EXPECT_TRUE(r1.has_value());

  // Second commit should be a safe no-op
  auto r2 = file.commit();
  EXPECT_TRUE(r2.has_value());
}

TEST_F(AtomicFileTest, IntegrationWithRecordWriterAndRecordReader) {
  const auto target = test_dir_ / "records.bin";

  // Write through AtomicFile using RecordWriter
  {
    AtomicFile file{target};
    RecordWriter writer{file.stream()};

    ASSERT_TRUE(writer.write_record("apple", "10").has_value());
    ASSERT_TRUE(writer.write_record("banana", "20").has_value());
    ASSERT_TRUE(writer.write_record("cherry", "30").has_value());

    ASSERT_TRUE(file.commit().has_value());
  }

  // Read back through RecordReader
  std::ifstream in{target, std::ios::binary};
  RecordReader reader{in};

  std::string k, v;

  auto r1 = reader.next_record(k, v);
  ASSERT_TRUE(r1.has_value() && *r1);
  EXPECT_EQ(k, "apple");
  EXPECT_EQ(v, "10");

  auto r2 = reader.next_record(k, v);
  ASSERT_TRUE(r2.has_value() && *r2);
  EXPECT_EQ(k, "banana");
  EXPECT_EQ(v, "20");

  auto r3 = reader.next_record(k, v);
  ASSERT_TRUE(r3.has_value() && *r3);
  EXPECT_EQ(k, "cherry");
  EXPECT_EQ(v, "30");

  auto r4 = reader.next_record(k, v);
  ASSERT_TRUE(r4.has_value() && !*r4); // Clean EOF
}

TEST_F(AtomicFileTest, MoveSelfAssignmentIsSafe) {
  const auto target = test_dir_ / "self_assign.bin";
  {
    AtomicFile file{target};
    file.stream() << "data";
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wself-move"
    file = std::move(file);
#pragma clang diagnostic pop
    EXPECT_TRUE(file.commit().has_value());
  }
  EXPECT_TRUE(std::filesystem::exists(target));
}

TEST_F(AtomicFileTest, RelativePathInCurrentDirectory) {
  const std::filesystem::path rel_target = "mr_test_rel_tmp.bin";
  std::filesystem::remove(rel_target);
  {
    AtomicFile file{rel_target};
    file.stream() << "content in current directory";
    auto res = file.commit();
    ASSERT_TRUE(res.has_value());
  }
  EXPECT_TRUE(std::filesystem::exists(rel_target));
  std::filesystem::remove(rel_target);
}

TEST_F(AtomicFileTest, IoErrorWhenStreamFailsBeforeCommit) {
  const auto target = test_dir_ / "bad_stream.bin";
  AtomicFile file{target};
  file.stream().setstate(std::ios::badbit);
  auto res = file.commit();
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), AtomicFile::ErrorState::IoError);
}

TEST_F(AtomicFileTest, AutoCreatesNestedParentDirectories) {
  const auto deep_target =
      test_dir_ / "level1" / "level2" / "level3" / "target.bin";
  EXPECT_FALSE(std::filesystem::exists(deep_target.parent_path()));
  {
    AtomicFile file{deep_target};
    EXPECT_TRUE(std::filesystem::exists(deep_target.parent_path()));
    file.stream() << "deeply nested";
    ASSERT_TRUE(file.commit().has_value());
  }
  EXPECT_TRUE(std::filesystem::exists(deep_target));
}

TEST_F(AtomicFileTest, LargePayloadExceedingBufferSize) {
  const auto target = test_dir_ / "large_payload.bin";
  std::string large_data(256 * 1024, 'X');
  {
    AtomicFile file{target, 32 * 1024}; // 32KB buffer
    file.stream().write(large_data.data(),
                        static_cast<std::streamsize>(large_data.size()));
    ASSERT_TRUE(file.commit().has_value());
  }
  EXPECT_EQ(std::filesystem::file_size(target), large_data.size());
  std::ifstream in{target, std::ios::binary};
  std::string read_back(large_data.size(), '\0');
  in.read(read_back.data(), static_cast<std::streamsize>(read_back.size()));
  EXPECT_EQ(read_back, large_data);
}

TEST_F(AtomicFileTest, RenameFailureHandling) {
  const auto target = test_dir_ / "target_dir";
  std::filesystem::create_directories(target);
  std::ofstream{target / "dummy.txt"} << "keep";

  std::filesystem::path staging;
  {
    AtomicFile file{target};
    staging = file.staging_path();
    file.stream() << "this should fail rename";
    auto res = file.commit();
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), AtomicFile::ErrorState::RenameFailed);
    EXPECT_FALSE(file.is_committed());
  }
  // Destructor should clean up the uncommitted staging file
  EXPECT_FALSE(std::filesystem::exists(staging));
}

} // namespace mr

