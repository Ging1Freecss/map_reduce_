#pragma once

#include <cstddef>
#include <expected>
#include <filesystem>
#include <fstream>
#include <ostream>
#include <vector>

namespace mr {
/*
    This is RAII transactional staging wrapper, designed to prevent currupt
    files

    - staging files are strictly in target_path.parent_path() this prevents
    error when calling rename() as it is an atomic swap need file on same
   directory

   - if exception occurs or a task fails before calling commit(), the destructor
    clean of staging file
*/
class AtomicFile {
private:
  std::filesystem::path target_path_;
  std::filesystem::path staging_path_;
  std::vector<std::byte> buffer_; // 64KB buffer
  std::ofstream file_stream_;
  bool committed_{false};

public:
  enum class ErrorState { IoError, RenameFailed };
  AtomicFile(const std::filesystem::path &target_path,
             std::size_t buffer_size = 64 * 1024);

  [[nodiscard]]
  auto stream() -> std::ostream &;
  [[nodiscard]] auto target_path() const noexcept
      -> const std::filesystem::path &;
  [[nodiscard]] auto staging_path() const noexcept
      -> const std::filesystem::path &;
  [[nodiscard]] auto is_committed() const noexcept -> bool;

  auto commit() -> std::expected<void, ErrorState>;

  ~AtomicFile();

  AtomicFile(const AtomicFile &) = delete;
  AtomicFile &operator=(const AtomicFile &) = delete;

  AtomicFile(AtomicFile &&other) noexcept
      : target_path_(std::move(other.target_path_)),
        staging_path_(std::move(other.staging_path_)),
        buffer_(std::move(other.buffer_)),
        file_stream_(std::move(other.file_stream_)),
        committed_(other.committed_) {
    /*
        important to do, so its destructor wont remove the
        staging_path_ file
    */
    other.committed_ = true;
  }

  AtomicFile &operator=(AtomicFile &&other) noexcept {
    if (this == &other) {
      return *this;
    }

    //  Clean up our own existing file if uncommitted (just like destructor):
    if (!committed_) {
      if (file_stream_.is_open()) {
        file_stream_.close();
      }
      std::error_code ec;
      std::filesystem::remove(staging_path_, ec);
    }

    target_path_ = std::move(other.target_path_);
    staging_path_ = std::move(other.staging_path_);
    buffer_ = std::move(other.buffer_);
    file_stream_ = std::move(other.file_stream_);
    committed_ = other.committed_;

    /*
        important to do, so its destructor wont remove the
        staging_path_ file
    */
    other.committed_ = true;

    return *this;
  }
};
} // namespace mr