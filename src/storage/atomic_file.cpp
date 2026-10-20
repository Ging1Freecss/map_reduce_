#include "storage/atomic_file.hpp"
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <system_error>
#include <unistd.h>

namespace mr {
static std::atomic<std::uint64_t> counter_{0};

AtomicFile::AtomicFile(const std::filesystem::path &target_path,
                       std::size_t buffer_size)
    : target_path_(target_path), buffer_(buffer_size, std::byte{}) {
  auto parent = target_path_.parent_path();

  if (!parent.empty()) {
    /**
        only create create directories if parent path is empty
        current_path() gives us absolute path

        AtomicFile{"result.bin"} -> parent = "/home/hp/Projects/map_reduce"

        so we cant do
         parent = std::filesystem::current_path();

        we need to assert
        staging.parent_path() == target.parent_path() as it is fundamental rule
       of atomic file staging is that the staging file and target file must
       reside in the same exact directory to prevent posix EXDEV cross
       filesystem error on rename

       so adding parent_path to staging_path will make
       staging.parent_path() != target.parent_path()


       Why we need to create directories
       - open cant create missing directories, it create only
         leaf file

        - in map reduce jobs are frequently generate nested directory for
       intermediate and final outputs
    */

    std::error_code ec;
    std::filesystem::create_directories(parent, ec);

    if (ec) {
      throw std::runtime_error("error in creating directories");
    }
  }

  std::filesystem::path staging_filename =
      ".tmp_stage_" + std::to_string(::getpid()) + "_" +
      std::to_string(counter_++) + "_" + target_path_.filename().string();

  staging_path_ = parent / staging_filename;

  file_stream_.rdbuf()->pubsetbuf(reinterpret_cast<char *>(buffer_.data()),
                                  buffer_.size());

  /*
  binary: prevent os from altering bytes
  out : specifies we intend to write bytes
  trunc: if a file already exist on disk,its length is truncated to 0 bytes ,
  fresh start, it only happen in open() not in write(), write() only appends the
  data
  */
  file_stream_.open(staging_path_,
                    std::ios::binary | std::ios::out | std::ios::trunc);

  if (!file_stream_.good()) {
    throw std::runtime_error("AtomicFile: failed to open staging file: " +
                             staging_path_.string());
  }
}

auto AtomicFile::commit() -> std::expected<void, ErrorState> {
  if (committed_)
    return {};

  /*
   on linux and windows , renaming an open file handle can cause locking
   or delayed deletion issues, so need to close before renaming
  */
  file_stream_.flush();
  if (!file_stream_.good())
    return std::unexpected(ErrorState::IoError);

  file_stream_.close();
  if (file_stream_.fail()) {
    return std::unexpected(ErrorState::IoError);
  }

  std::error_code ec;
  std::filesystem::rename(staging_path_, target_path_, ec);

  if (ec)
    return std::unexpected(ErrorState::RenameFailed);

  committed_ = true;
  return {};
}

AtomicFile::~AtomicFile() {
  if (!committed_) {
    if (file_stream_.is_open()) {
      file_stream_.close();
    }
    std::error_code ec;
    std::filesystem::remove(staging_path_, ec);
  }
}

auto AtomicFile::stream() -> std::ostream & { return file_stream_; }

auto AtomicFile::target_path() const noexcept -> const std::filesystem::path & {
  return target_path_;
}
auto AtomicFile::staging_path() const noexcept
    -> const std::filesystem::path & {
  return staging_path_;
}
auto AtomicFile::is_committed() const noexcept -> bool { return committed_; }
} // namespace mr