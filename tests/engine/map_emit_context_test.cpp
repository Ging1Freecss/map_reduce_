#include "engine/map_emit_context.hpp"
#include "core/emit_context.hpp"
#include "core/mapper.hpp"
#include "core/partitioner.hpp"
#include "storage/record_reader.hpp"
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace mr {
namespace {

class MapEmitContextTest : public ::testing::Test {
protected:
  std::filesystem::path test_dir_;

  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() /
                ("mr_map_emit_test_" + std::to_string(::getpid()) + "_" +
                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(test_dir_, ec);
  }
};

// Custom partitioner for testing
struct FixedPartitioner {
  std::size_t target_partition{0};

  auto partition([[maybe_unused]] std::string_view key,
                 [[maybe_unused]] std::size_t num_partitions) const -> std::size_t {
    return target_partition;
  }
};
static_assert(IsPartitioner<FixedPartitioner>);

// Mock emitter for unit testing WordCountMapper in isolation
struct MockEmitter {
  std::vector<std::pair<std::string, std::string>> emitted;
  bool fail_on_emit{false};

  auto emit(std::string_view key, std::string_view value)
      -> std::expected<void, StorageError> {
    if (fail_on_emit) {
      return std::unexpected(StorageError::IoError);
    }
    emitted.emplace_back(std::string(key), std::string(value));
    return {};
  }
};
static_assert(IsEmitter<MockEmitter>);

} // namespace

TEST_F(MapEmitContextTest, PartitionAndCommitRecords) {
  const std::size_t map_id = 0;
  const std::size_t num_reducers = 3;

  std::vector<std::pair<std::string, std::string>> records = {
      {"apple", "1"}, {"banana", "2"}, {"cherry", "3"},
      {"date", "4"},  {"elderberry", "5"}, {"fig", "6"}, {"grape", "7"}};

  HashPartitioner partitioner;
  std::unordered_map<std::size_t, std::vector<std::pair<std::string, std::string>>> expected_per_partition;
  for (const auto &[k, v] : records) {
    std::size_t p = partitioner.partition(k, num_reducers);
    expected_per_partition[p].emplace_back(k, v);
  }

  {
    MapEmitContext<HashPartitioner> ctx{map_id, num_reducers, test_dir_};

    for (const auto &[k, v] : records) {
      auto res = ctx.emit(k, v);
      ASSERT_TRUE(res.has_value());
    }

    auto commit_res = ctx.commit();
    ASSERT_TRUE(commit_res.has_value());
  }

  // Verify all partition files exist and contain the expected records
  for (std::size_t i = 0; i < num_reducers; ++i) {
    std::filesystem::path partition_file =
        test_dir_ / ("mr-" + std::to_string(map_id) + "_" + std::to_string(i) + ".data");
    ASSERT_TRUE(std::filesystem::exists(partition_file));

    std::ifstream in{partition_file, std::ios::binary};
    RecordReader reader{in};

    std::vector<std::pair<std::string, std::string>> actual_records;
    std::string key, value;
    while (true) {
      auto next_res = reader.next_record(key, value);
      ASSERT_TRUE(next_res.has_value());
      if (!*next_res) {
        break;
      }
      actual_records.emplace_back(key, value);
    }

    EXPECT_EQ(actual_records, expected_per_partition[i]);
  }
}

TEST_F(MapEmitContextTest, CustomPartitionerRouting) {
  const std::size_t map_id = 42;
  const std::size_t num_reducers = 4;
  const std::size_t target_p = 2;

  FixedPartitioner custom_partitioner{target_p};

  {
    MapEmitContext<FixedPartitioner> ctx{map_id, num_reducers, test_dir_, custom_partitioner};

    EXPECT_TRUE(ctx.emit("alpha", "10").has_value());
    EXPECT_TRUE(ctx.emit("beta", "20").has_value());
    EXPECT_TRUE(ctx.emit("gamma", "30").has_value());

    EXPECT_TRUE(ctx.commit().has_value());
  }

  for (std::size_t i = 0; i < num_reducers; ++i) {
    std::filesystem::path partition_file =
        test_dir_ / ("mr-" + std::to_string(map_id) + "_" + std::to_string(i) + ".data");
    ASSERT_TRUE(std::filesystem::exists(partition_file));

    std::ifstream in{partition_file, std::ios::binary};
    RecordReader reader{in};
    std::string k, v;
    std::size_t count = 0;
    while (true) {
      auto next_res = reader.next_record(k, v);
      ASSERT_TRUE(next_res.has_value());
      if (!*next_res) {
        break;
      }
      count++;
    }

    if (i == target_p) {
      EXPECT_EQ(count, 3u);
    } else {
      EXPECT_EQ(count, 0u);
    }
  }
}

TEST_F(MapEmitContextTest, WordCountMapperTokenization) {
  WordCountMapper mapper;
  MockEmitter emitter;

  std::string_view text = "  Hello   world! \t This is   MapReduce \n test.  ";
  auto res = mapper.map("key_ignored", text, emitter);
  ASSERT_TRUE(res.has_value());

  std::vector<std::string> expected_words = {"Hello", "world!", "This", "is", "MapReduce", "test."};
  ASSERT_EQ(emitter.emitted.size(), expected_words.size());

  for (std::size_t i = 0; i < expected_words.size(); ++i) {
    EXPECT_EQ(emitter.emitted[i].first, expected_words[i]);
    EXPECT_EQ(emitter.emitted[i].second, "1");
  }
}

TEST_F(MapEmitContextTest, WordCountMapperEmptyAndWhitespace) {
  WordCountMapper mapper;

  // Empty string
  {
    MockEmitter emitter;
    auto res = mapper.map("k", "", emitter);
    EXPECT_TRUE(res.has_value());
    EXPECT_TRUE(emitter.emitted.empty());
  }

  // Whitespace only
  {
    MockEmitter emitter;
    auto res = mapper.map("k", "   \t\t \n \r  ", emitter);
    EXPECT_TRUE(res.has_value());
    EXPECT_TRUE(emitter.emitted.empty());
  }
}

TEST_F(MapEmitContextTest, WordCountMapperErrorPropagation) {
  WordCountMapper mapper;
  MockEmitter emitter;
  emitter.fail_on_emit = true;

  auto res = mapper.map("k", "failure should occur on first token", emitter);
  EXPECT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), StorageError::IoError);
}

TEST_F(MapEmitContextTest, WordCountMapperEndToEndWithMapEmitContext) {
  const std::size_t map_id = 7;
  const std::size_t num_reducers = 2;

  MapEmitContext<HashPartitioner> ctx{map_id, num_reducers, test_dir_};
  WordCountMapper mapper;

  auto map_res = mapper.map("line1", "quick brown fox jumps", ctx);
  ASSERT_TRUE(map_res.has_value());

  auto commit_res = ctx.commit();
  ASSERT_TRUE(commit_res.has_value());

  // Count total records across all partitions
  std::size_t total_records = 0;
  for (std::size_t i = 0; i < num_reducers; ++i) {
    std::filesystem::path partition_file =
        test_dir_ / ("mr-" + std::to_string(map_id) + "_" + std::to_string(i) + ".data");
    ASSERT_TRUE(std::filesystem::exists(partition_file));

    std::ifstream in{partition_file, std::ios::binary};
    RecordReader reader{in};
    std::string k, v;
    while (true) {
      auto next_res = reader.next_record(k, v);
      ASSERT_TRUE(next_res.has_value());
      if (!*next_res) {
        break;
      }
      EXPECT_EQ(v, "1");
      total_records++;
    }
  }

  EXPECT_EQ(total_records, 4u);
}

} // namespace mr
