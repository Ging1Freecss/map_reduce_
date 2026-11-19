#pragma once

#include "core/value_iterator.hpp"
#include "storage/error.hpp"
#include "storage/record_reader.hpp"
#include "storage/staged_record_writer.hpp"
#include <cstddef>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <queue>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace mr {

struct HeapNode {
  std::string key;
  std::string value;
  std::size_t reader_idx;

  bool operator>(const HeapNode &other) const {
    if (key != other.key) {
      return key > other.key;
    }
    return value > other.value;
  }

  bool operator<(const HeapNode &other) const {
    if (key != other.key) {
      return key < other.key;
    }
    return value < other.value;
  }

  bool operator==(const HeapNode &other) const {
    return value == other.value && key == other.key;
  }
};

using MinHeap = std::priority_queue<HeapNode, std::vector<HeapNode>,
                                    std::greater<HeapNode>>;

/*
  algo:
  1. first sort indivisual file using in memory sort
  2. then put first element of each file into heap (key,value,index) here
     index refer to index of file in readers_ vector
  3. then get the shortest element in the heap, get the index get the next
  element in readers_[index] and so on

  4.Why Keys Never Interleave:
    Because files are sorted, future unread records in any file are always >=
    the record currently in the heap. Therefore, popped keys are monotonic
    guaranteeing all identical keys appear consecutively without gaps.

*/
class StreamingValueIterator {
private:
  MinHeap heap_;
  std::string current_key_;
  std::string current_val_;
  std::size_t consumed_count_{0};
  std::optional<StorageError> error_;
  std::vector<std::unique_ptr<std::ifstream>> files_;
  std::vector<std::unique_ptr<RecordReader>> readers_;

public:
  StreamingValueIterator(const std::vector<std::filesystem::path> &file_paths) {

    readers_.reserve(file_paths.size());
    files_.reserve(file_paths.size());

    for (std::size_t i = 0; i < file_paths.size(); i++) {
      if (!std::filesystem::is_regular_file(file_paths[i])) {
        throw std::runtime_error("Can't find file" +
                                 std::string{file_paths[i].filename()} +
                                 " in StreamingValueIterator");
      }

      auto sort_res = sort_file_inplace(file_paths[i]);
      if (!sort_res) {
        throw std::runtime_error("Error in sorting " +
                                 std::string{file_paths[i].filename()} +
                                 " in StreamingValueIterator");
      }

      auto file = std::make_unique<std::ifstream>(file_paths[i]);

      if (!file->is_open()) {
        throw std::runtime_error("Can't open file" +
                                 std::string{file_paths[i].filename()} +
                                 " in StreamingValueIterator");
      }

      auto reader = std::make_unique<RecordReader>(*file);
      files_.push_back(std::move(file));
      readers_.push_back(std::move(reader));
    }

    // put first {key,value} of each file into heap
    for (std::size_t i = 0; i < readers_.size(); i++) {
      std::string k, v;

      auto res = readers_[i]->next_record(k, v);

      if (!res) {
        throw std::runtime_error("issues with reading " +
                                 std::string{file_paths[i].filename()} +
                                 " in StreamingValueIterator");
      }

      if (res.value()) {
        heap_.push(HeapNode{std::move(k), std::move(v), i});
      }
    }

    if (!heap_.empty()) {
      current_key_ = heap_.top().key;
    }
  }

  /*
    open in the file, read and put the data in a buffer
    sort it, then write to the same file

    64mb-128mb so sorting in buffer will not cause issue
  */
  static auto sort_file_inplace(const std::filesystem::path &path)
      -> std::expected<void, StorageError> {
    std::vector<std::pair<std::string, std::string>> buffer;
    {
      std::ifstream in(path);

      if (!in.is_open())
        throw std::runtime_error("Can't open file" +
                                 std::string{path.filename()} +
                                 " in StreamingValueIterator");

      RecordReader reader{in};
      std::string key, value;

      while (true) {
        auto res = reader.next_record(key, value);

        if (!res)
          throw std::runtime_error("issue with reading file");

        if (!res.value())
          break; // EOF

        buffer.emplace_back(std::move(key), std::move(value));
      }
    } // closes in before writing

    std::sort(
        buffer.begin(), buffer.end(),
        [](const auto &a, const auto &b) -> bool { return a.first < b.first; });

    StagedRecordWriter writer{path};
    for (const auto &[k, v] : buffer) {
      auto w_res = writer.write_record(k, v);
      if (!w_res)
        throw std::runtime_error("issue with writing in sort_file_unplace");
    }

    return writer.commit();
  }

  // check if the top key in the mean heap == current_key
  auto has_next() -> bool {
    return !error_.has_value() && !heap_.empty() &&
           heap_.top().key == current_key_;
  }

  // drain the key that are equat to current_key_
  auto next() -> std::string_view {
    /*
      std::priority_queue::top() returns a const HeapNode&,
      calling move on const reference cast it to const HeapNode&&
      which call copy constructor, to move it we have remove the
      const part
    */
    HeapNode top = std::move(const_cast<HeapNode &>(heap_.top()));
    heap_.pop();

    current_val_ = std::move(top.value);
    std::string k, v;
    auto res = readers_[top.reader_idx]->next_record(k, v);

    if (!res) {
      throw std::runtime_error("issue in reading , in next()");
    } else if (!res.value()) {
      // EOF reach ignore
    } else {
      heap_.push(HeapNode{std::move(k), std::move(v), top.reader_idx});
    }
    consumed_count_++;
    return current_val_;
  }

  // update current_key_ and return number of pairs consume
  // note this is here to satisfy IsValueIterator
  auto current_index() -> std::size_t {

    /*
      this consume the key that are same as current_key_
      note this loop will not run if in the
      user code we consume first, when running current_index
      it just assumes that you want the next key
    */

    while (has_next()) {
      next();
    }

    if (!heap_.empty()) {
      current_key_ = heap_.top().key;
    } else {
      current_key_.clear();
    }

    std::size_t count = consumed_count_;
    consumed_count_ = 0; // Reset for next key
    return count;
  }

  auto current_key() const -> std::string_view { return current_key_; }

  [[nodiscard]] auto has_more_keys() const noexcept -> bool {
    return !heap_.empty();
  }
};

static_assert(IsValueIterator<StreamingValueIterator>,
              "class is not a value iterator");
} // namespace mr
