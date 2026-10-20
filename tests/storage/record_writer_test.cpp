#include "storage/endian.hpp"
#include "storage/record_writer.hpp"
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>

namespace mr {
namespace {

// Helper to view a string or binary string as a span of const std::byte
auto to_bytes(std::string_view sv) noexcept -> std::span<const std::byte> {
  return std::as_bytes(std::span{sv.data(), sv.size()});
}

// Helper to view a span of const std::byte as a string_view
auto to_string_view(std::span<const std::byte> bytes) noexcept
    -> std::string_view {
  return {reinterpret_cast<const char *>(bytes.data()), bytes.size()};
}

} // namespace

// ============================================================================
// RecordWriter Unit Tests
// ============================================================================

TEST(RecordWriterTest, ConstructorThrowsOnBadStream) {
  std::ostringstream oss;
  oss.setstate(std::ios_base::badbit);
  EXPECT_THROW((RecordWriter{oss}), std::runtime_error);
}

TEST(RecordWriterTest, WriteStringRecord) {
  std::ostringstream oss;
  RecordWriter writer{oss};

  auto res = writer.write_record("user_id", "42");
  ASSERT_TRUE(res.has_value());

  std::string data = oss.str();
  Packet packet(to_bytes(data));
  auto record = packet.deserialize_data();
  ASSERT_TRUE(record.has_value());

  auto [key_span, val_span] = *record;
  EXPECT_EQ(to_string_view(key_span), "user_id");
  EXPECT_EQ(to_string_view(val_span), "42");
}

TEST(RecordWriterTest, WriteBinaryRecord) {
  std::ostringstream oss;
  RecordWriter writer{oss};

  const std::string bin_key("bin\0key", 7);
  const std::string bin_val("val\xFF\x00end", 8);

  auto res = writer.write_record(to_bytes(bin_key), to_bytes(bin_val));
  ASSERT_TRUE(res.has_value());

  std::string data = oss.str();
  Packet packet(to_bytes(data));
  auto record = packet.deserialize_data();
  ASSERT_TRUE(record.has_value());

  auto [key_span, val_span] = *record;
  EXPECT_EQ(to_string_view(key_span), bin_key);
  EXPECT_EQ(to_string_view(val_span), bin_val);
}

TEST(RecordWriterTest, WritePacketDirectly) {
  std::ostringstream oss;
  RecordWriter writer{oss};

  Packet packet;
  packet.serialise_data(to_bytes("direct"), to_bytes("packet"));

  auto res = writer.write_packet(packet);
  ASSERT_TRUE(res.has_value());

  std::string data = oss.str();
  Packet read_back(to_bytes(data));
  auto record = read_back.deserialize_data();
  ASSERT_TRUE(record.has_value());

  auto [key_span, val_span] = *record;
  EXPECT_EQ(to_string_view(key_span), "direct");
  EXPECT_EQ(to_string_view(val_span), "packet");
}

TEST(RecordWriterTest, EmptyKeyAndValue) {
  std::ostringstream oss;
  RecordWriter writer{oss};

  auto res = writer.write_record("", "");
  ASSERT_TRUE(res.has_value());

  std::string data = oss.str();
  Packet packet(to_bytes(data));
  auto record = packet.deserialize_data();
  ASSERT_TRUE(record.has_value());

  auto [key_span, val_span] = *record;
  EXPECT_TRUE(key_span.empty());
  EXPECT_TRUE(val_span.empty());
}

TEST(RecordWriterTest, MultipleRecordsSequential) {
  std::ostringstream oss;
  RecordWriter writer{oss};

  ASSERT_TRUE(writer.write_record("k1", "v1").has_value());
  ASSERT_TRUE(writer.write_record("k2", "v2").has_value());

  std::string raw = oss.str();
  std::span<const std::byte> total_bytes = to_bytes(raw);

  // Packet format: 4 bytes (key_len) + key + 4 bytes (val_len) + value
  constexpr std::size_t rec1_len = sizeof(std::uint32_t) + 2 + sizeof(std::uint32_t) + 2;
  ASSERT_GE(total_bytes.size(), rec1_len * 2);

  Packet p1(total_bytes.subspan(0, rec1_len));
  auto rec1 = p1.deserialize_data();
  ASSERT_TRUE(rec1.has_value());
  EXPECT_EQ(to_string_view(std::get<0>(*rec1)), "k1");
  EXPECT_EQ(to_string_view(std::get<1>(*rec1)), "v1");

  Packet p2(total_bytes.subspan(rec1_len, rec1_len));
  auto rec2 = p2.deserialize_data();
  ASSERT_TRUE(rec2.has_value());
  EXPECT_EQ(to_string_view(std::get<0>(*rec2)), "k2");
  EXPECT_EQ(to_string_view(std::get<1>(*rec2)), "v2");
}

TEST(RecordWriterTest, FlushSuccess) {
  std::ostringstream oss;
  RecordWriter writer{oss};

  ASSERT_TRUE(writer.write_record("k", "v").has_value());
  auto flush_res = writer.flush();
  EXPECT_TRUE(flush_res.has_value());
}

TEST(RecordWriterTest, IoErrorOnBadStreamDuringWrite) {
  std::ostringstream oss;
  RecordWriter writer{oss};

  // Corrupt stream state after construction
  oss.setstate(std::ios_base::badbit);

  auto res = writer.write_record("k", "v");
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), RecordWriter::ErrorState::IoError);
}

TEST(RecordWriterTest, IoErrorOnBadStreamDuringFlush) {
  std::ostringstream oss;
  RecordWriter writer{oss};

  oss.setstate(std::ios_base::badbit);
  auto flush_res = writer.flush();
  ASSERT_FALSE(flush_res.has_value());
  EXPECT_EQ(flush_res.error(), RecordWriter::ErrorState::IoError);
}

TEST(RecordWriterTest, KeyOrValueLengthMaxExceeded) {
  std::ostringstream oss;
  RecordWriter writer{oss};

  constexpr std::size_t too_large =
      static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max()) + 1;
  std::span<const std::byte> huge_span{static_cast<const std::byte *>(nullptr),
                                       too_large};

  // Key too large
  auto res_key = writer.write_record(huge_span, to_bytes("val"));
  ASSERT_FALSE(res_key.has_value());
  EXPECT_EQ(res_key.error(), RecordWriter::ErrorState::KeyOrValueLengthMaxExceeded);

  // Value too large
  auto res_val = writer.write_record(to_bytes("key"), huge_span);
  ASSERT_FALSE(res_val.has_value());
  EXPECT_EQ(res_val.error(), RecordWriter::ErrorState::KeyOrValueLengthMaxExceeded);
}

TEST(RecordWriterTest, MoveSemantics) {
  std::ostringstream oss;
  RecordWriter writer{oss};

  RecordWriter moved_writer = std::move(writer);
  auto res = moved_writer.write_record("moved_k", "moved_v");
  ASSERT_TRUE(res.has_value());

  Packet p(to_bytes(oss.str()));
  auto rec = p.deserialize_data();
  ASSERT_TRUE(rec.has_value());
  EXPECT_EQ(to_string_view(std::get<0>(*rec)), "moved_k");
  EXPECT_EQ(to_string_view(std::get<1>(*rec)), "moved_v");
}

} // namespace mr
