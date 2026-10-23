#include "engine/reduce_emit_context.hpp"
#include "engine/vector_value_iterator.hpp"
#include "engine/sequential_harness.hpp"
#include "core/reducer.hpp"
#include "core/value_iterator.hpp"
#include "storage/record_reader.hpp"
#include "storage/staged_record_writer.hpp"
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <utility>
#include <vector>

namespace mr {
namespace {

class ReducePhaseTest : public ::testing::Test {
protected:
  std::filesystem::path test_dir_;

  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() /
                ("mr_reduce_test_" + std::to_string(::getpid()) + "_" +
                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(test_dir_, ec);
  }
};

// 1. VectorValueIterator Tests
TEST_F(ReducePhaseTest, VectorValueIteratorBasicIteration) {
  std::vector<std::pair<std::string, std::string>> records = {
      {"apple", "1"},
      {"apple", "1"},
      {"apple", "1"},
      {"banana", "1"},
      {"cherry", "2"}
  };

  VectorValueIterator iter(records, "apple", 0);

  EXPECT_TRUE(iter.has_next());
  EXPECT_EQ(iter.next(), "1");
  EXPECT_TRUE(iter.has_next());
  EXPECT_EQ(iter.next(), "1");
  EXPECT_TRUE(iter.has_next());
  EXPECT_EQ(iter.next(), "1");

  EXPECT_FALSE(iter.has_next());
  EXPECT_EQ(iter.current_index(), 3); // Index of "banana"
}

TEST_F(ReducePhaseTest, VectorValueIteratorEarlyExitSkipsSameKey) {
  std::vector<std::pair<std::string, std::string>> records = {
      {"apple", "1"},
      {"apple", "2"},
      {"apple", "3"},
      {"banana", "1"}
  };

  VectorValueIterator iter(records, "apple", 0);
  EXPECT_TRUE(iter.has_next());
  EXPECT_EQ(iter.next(), "1"); // Only consume 1 value, then stop

  // current_index() should automatically skip the remaining "apple" records
  EXPECT_EQ(iter.current_index(), 3); // Must point to "banana"
}

TEST_F(ReducePhaseTest, VectorValueIteratorEmptyOrMismatch) {
  std::vector<std::pair<std::string, std::string>> records = {
      {"banana", "1"}
  };

  VectorValueIterator iter(records, "apple", 0);
  EXPECT_FALSE(iter.has_next());
  EXPECT_EQ(iter.current_index(), 0);
}

// 2. ReduceEmitContext Tests
TEST_F(ReducePhaseTest, ReduceEmitContextWritesAndCommits) {
  const std::size_t reduce_id = 0;
  {
    ReduceEmitContext ctx(reduce_id, test_dir_);
    auto res1 = ctx.emit("apple", "3");
    ASSERT_TRUE(res1.has_value());

    auto res2 = ctx.emit("banana", "5");
    ASSERT_TRUE(res2.has_value());

    auto commit_res = ctx.commit();
    ASSERT_TRUE(commit_res.has_value());
  }

  // Verify the canonical output file mr-out-0 exists and contains the records
  auto out_path = test_dir_ / ("mr-out-" + std::to_string(reduce_id));
  ASSERT_TRUE(std::filesystem::exists(out_path));

  std::ifstream in(out_path);
  ASSERT_TRUE(in.is_open());
  RecordReader reader(in);

  std::string k, v;
  auto r1 = reader.next_record(k, v);
  ASSERT_TRUE(r1.has_value() && *r1);
  EXPECT_EQ(k, "apple");
  EXPECT_EQ(v, "3");

  auto r2 = reader.next_record(k, v);
  ASSERT_TRUE(r2.has_value() && *r2);
  EXPECT_EQ(k, "banana");
  EXPECT_EQ(v, "5");

  auto r3 = reader.next_record(k, v);
  ASSERT_TRUE(r3.has_value());
  EXPECT_FALSE(*r3); // EOF
}

// 3. WordCountReducer Tests
TEST_F(ReducePhaseTest, WordCountReducerAggregatesCounts) {
  std::vector<std::pair<std::string, std::string>> records = {
      {"apple", "1"},
      {"apple", "2"},
      {"apple", "4"}
  };

  ReduceEmitContext ctx(0, test_dir_);
  WordCountReducer reducer;
  VectorValueIterator iter(records, "apple", 0);

  auto res = reducer.reduce("apple", iter, ctx);
  ASSERT_TRUE(res.has_value());
  ASSERT_TRUE(ctx.commit().has_value());

  std::ifstream in(test_dir_ / "mr-out-0");
  RecordReader reader(in);
  std::string k, v;
  auto r = reader.next_record(k, v);
  ASSERT_TRUE(r.has_value() && *r);
  EXPECT_EQ(k, "apple");
  EXPECT_EQ(v, "7"); // 1 + 2 + 4 = 7
}

// 4. End-to-End run_reduce_phase Test (Multi-Mapper input)
TEST_F(ReducePhaseTest, RunReducePhaseEndToEndMultipleMappers) {
  const std::size_t reduce_id = 0;
  const std::size_t num_maps = 2;

  // Mapper 0 writes: ("apple", "1"), ("banana", "1") to mr-0_0.data
  {
    StagedRecordWriter writer(test_dir_ / "mr-0_0.data");
    ASSERT_TRUE(writer.write_record("apple", "1").has_value());
    ASSERT_TRUE(writer.write_record("banana", "1").has_value());
    ASSERT_TRUE(writer.commit().has_value());
  }

  // Mapper 1 writes: ("apple", "2"), ("cherry", "3") to mr-1_0.data
  {
    StagedRecordWriter writer(test_dir_ / "mr-1_0.data");
    ASSERT_TRUE(writer.write_record("apple", "2").has_value());
    ASSERT_TRUE(writer.write_record("cherry", "3").has_value());
    ASSERT_TRUE(writer.commit().has_value());
  }

  // Run the reduce phase for partition 0 across all 2 mappers
  WordCountReducer reducer;
  auto res = run_reduce_phase(test_dir_, reduce_id, num_maps, reducer);
  ASSERT_TRUE(res.has_value());

  // Verify mr-out-0 contents:
  // Expected:
  // "apple" -> 1 + 2 = 3
  // "banana" -> 1
  // "cherry" -> 3
  std::ifstream in(test_dir_ / "mr-out-0");
  ASSERT_TRUE(in.is_open());
  RecordReader reader(in);

  std::string k, v;

  // Record 1: apple
  auto r1 = reader.next_record(k, v);
  ASSERT_TRUE(r1.has_value() && *r1);
  EXPECT_EQ(k, "apple");
  EXPECT_EQ(v, "3");

  // Record 2: banana
  auto r2 = reader.next_record(k, v);
  ASSERT_TRUE(r2.has_value() && *r2);
  EXPECT_EQ(k, "banana");
  EXPECT_EQ(v, "1");

  // Record 3: cherry
  auto r3 = reader.next_record(k, v);
  ASSERT_TRUE(r3.has_value() && *r3);
  EXPECT_EQ(k, "cherry");
  EXPECT_EQ(v, "3");

  // EOF
  auto r4 = reader.next_record(k, v);
  ASSERT_TRUE(r4.has_value());
  EXPECT_FALSE(*r4);
}

// 5. Full End-to-End Map + Reduce Pipeline Integration
TEST_F(ReducePhaseTest, FullSequentialMapReducePipeline) {
  // 1. Create a sample text file
  auto input_file = test_dir_ / "sample.txt";
  {
    std::ofstream out(input_file);
    out << "apple banana apple\n";
    out << "banana cherry apple\n";
    out << "date fig grape\n";
    out << "apple banana\n";
  }

  const std::size_t num_reducers = 4;

  // 2. Run the complete pipeline (Map + Reduce)
  auto res = run_sequential_map_reduce(input_file, test_dir_, num_reducers);
  ASSERT_TRUE(res.has_value());

  // 3. Verify all intermediate files were generated
  for (std::size_t r = 0; r < num_reducers; ++r) {
    EXPECT_TRUE(std::filesystem::exists(test_dir_ / ("mr-0_" + std::to_string(r) + ".data")));
  }

  // 4. Verify all canonical output partitions exist and aggregate counts
  std::unordered_map<std::string, std::size_t> total_counts;

  for (std::size_t r = 0; r < num_reducers; ++r) {
    auto out_file = test_dir_ / ("mr-out-" + std::to_string(r));
    ASSERT_TRUE(std::filesystem::exists(out_file));

    std::ifstream in(out_file);
    RecordReader reader(in);
    std::string word, count_str;
    while (true) {
      auto rec = reader.next_record(word, count_str);
      ASSERT_TRUE(rec.has_value());
      if (!*rec) break;

      total_counts[word] += std::stoul(count_str);
    }
  }

  // 5. Verify the word frequencies
  EXPECT_EQ(total_counts["apple"], 4);
  EXPECT_EQ(total_counts["banana"], 3);
  EXPECT_EQ(total_counts["cherry"], 1);
  EXPECT_EQ(total_counts["date"], 1);
  EXPECT_EQ(total_counts["fig"], 1);
  EXPECT_EQ(total_counts["grape"], 1);
  EXPECT_EQ(total_counts.size(), 6);
}

} // namespace
} // namespace mr
