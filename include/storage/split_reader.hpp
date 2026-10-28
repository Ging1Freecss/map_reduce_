#pragma once

#include "storage/error.hpp"
#include <cstdint>
#include <expected>
#include <filesystem>
#include <fstream>
#include <optional>
#include <vector>
namespace mr {

// byte range assignment given to individual Mapper task
struct InputSplit {
  std::filesystem::path file_path;
  std::uint64_t offset{0};
  std::uint64_t length{0};
};

auto generate_binary_splits(const std::filesystem::path &file_path,
                            std::uint64_t target_split_bytes)
    -> std::expected<std::vector<InputSplit>, StorageError>;

auto generate_text_splits(const std::filesystem::path &file_path,
                          uint64_t target_split_bytes = 64 * 1024 * 1024)
    -> std::expected<std::vector<InputSplit>, StorageError>;

class SplitReader {
private:
  std::ifstream file_;
  std::optional<std::uint64_t> max_bytes_;
  std::uint64_t bytes_read_{0};

public:
  using ErrorState = StorageError;

  SplitReader(const std::filesystem::path &file_path,
              std::uint64_t start_offset = 0,
              std::optional<std::uint64_t> max_bytes = std::nullopt);

  auto next_record(std::string &key, std::string &val)
      -> std::expected<bool, ErrorState>;

  [[nodiscard]] std::uint64_t bytes_read() const noexcept;
};
} // namespace mr