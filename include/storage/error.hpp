#pragma once

#include <string_view>

namespace mr {

enum class StorageError {
  IoError,
  RenameFailed,
  KeyOrValueLengthMaxExceeded,
  TruncatedHeader,
  TruncatedPayload,
  SplitQuotaEnd,
};

constexpr auto to_string(StorageError err) noexcept -> std::string_view {
  switch (err) {
  case StorageError::IoError:
    return "I/O error";
  case StorageError::RenameFailed:
    return "Rename failed";
  case StorageError::KeyOrValueLengthMaxExceeded:
    return "Key or value length exceeded 4GB maximum";
  case StorageError::TruncatedHeader:
    return "Truncated record header";
  case StorageError::TruncatedPayload:
    return "Truncated record payload";

  case StorageError::SplitQuotaEnd:
    return "Split reader has read the target bytes";
  }
  return "Unknown storage error";
}

} // namespace mr
