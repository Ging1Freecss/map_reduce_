#include <gtest/gtest.h>
#include "storage/endian.hpp"

#include <bit>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace {

// Helper to view a string or binary string as a span of const std::byte
auto to_bytes(std::string_view sv) noexcept -> std::span<const std::byte> {
    return std::as_bytes(std::span{sv.data(), sv.size()});
}

// Helper to view a span of const std::byte as a string_view
auto to_string_view(std::span<const std::byte> bytes) noexcept -> std::string_view {
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

} // namespace

// ============================================================================
// Endianness Unit Tests
// ============================================================================

TEST(EndianTest, RoundTripSymmetry) {
    constexpr std::uint32_t values[] = {
        0x00000000U,
        0x00000001U,
        0x12345678U,
        0xAABBCCDDU,
        0xFFFFFFFFU
    };

    for (const auto val : values) {
        std::uint32_t encoded = to_little_endian_32(val);
        std::uint32_t decoded = from_little_endian_32(encoded);
        EXPECT_EQ(decoded, val);
    }
}

TEST(EndianTest, DeterministicLittleEndianLayout) {
    constexpr std::uint32_t val = 0x12345678U;
    std::uint32_t le = to_little_endian_32(val);

    const auto* bytes = reinterpret_cast<const std::uint8_t*>(&le);
    // In little-endian, LSB (0x78) is stored at lowest address
    EXPECT_EQ(bytes[0], 0x78);
    EXPECT_EQ(bytes[1], 0x56);
    EXPECT_EQ(bytes[2], 0x34);
    EXPECT_EQ(bytes[3], 0x12);
}

// ============================================================================
// Packet Serialization & Deserialization Tests
// ============================================================================

TEST(PacketTest, RoundTripStandardKeyValue) {
    Packet packet;
    packet.serialise_data(to_bytes("word"), to_bytes("1"));

    auto result = packet.deserialize_data();
    ASSERT_TRUE(result.has_value());

    auto [key_span, value_span] = *result;
    EXPECT_EQ(to_string_view(key_span), "word");
    EXPECT_EQ(to_string_view(value_span), "1");
}

TEST(PacketTest, RoundTripArbitraryBinaryAndNullBytes) {
    // Keys and values with embedded null bytes and non-ASCII binary values
    const std::string binary_key("prefix\0mid\0suffix", 17);
    const std::string binary_value("val\0\xFF\xFE\0end", 11);

    Packet packet;
    packet.serialise_data(to_bytes(binary_key), to_bytes(binary_value));

    auto result = packet.deserialize_data();
    ASSERT_TRUE(result.has_value());

    auto [key_span, value_span] = *result;
    EXPECT_EQ(to_string_view(key_span), binary_key);
    EXPECT_EQ(to_string_view(value_span), binary_value);
}

TEST(PacketTest, EmptyKeyAndEmptyValueHandling) {
    // 1. Empty key with valid value
    {
        Packet packet;
        packet.serialise_data(to_bytes(""), to_bytes("global_aggregation"));

        auto result = packet.deserialize_data();
        ASSERT_TRUE(result.has_value());
        auto [k, v] = *result;
        EXPECT_TRUE(k.empty());
        EXPECT_EQ(to_string_view(v), "global_aggregation");
    }

    // 2. Valid key with empty value
    {
        Packet packet;
        packet.serialise_data(to_bytes("unique_key"), to_bytes(""));

        auto result = packet.deserialize_data();
        ASSERT_TRUE(result.has_value());
        auto [k, v] = *result;
        EXPECT_EQ(to_string_view(k), "unique_key");
        EXPECT_TRUE(v.empty());
    }

    // 3. Both empty
    {
        Packet packet;
        packet.serialise_data(to_bytes(""), to_bytes(""));

        auto result = packet.deserialize_data();
        ASSERT_TRUE(result.has_value());
        auto [k, v] = *result;
        EXPECT_TRUE(k.empty());
        EXPECT_TRUE(v.empty());
        // 4 bytes key_len (0) + 4 bytes val_len (0) = 8 bytes total
        EXPECT_EQ(packet.size(), 8U);
    }
}

TEST(PacketTest, LargePayload) {
    const std::string large_key(1024, 'K');
    const std::string large_value(64 * 1024, 'V'); // 64 KB

    Packet packet;
    packet.serialise_data(to_bytes(large_key), to_bytes(large_value));

    auto result = packet.deserialize_data();
    ASSERT_TRUE(result.has_value());

    auto [k, v] = *result;
    EXPECT_EQ(to_string_view(k), large_key);
    EXPECT_EQ(to_string_view(v), large_value);
}

// ============================================================================
// Truncation & Corrupted Data Detection Tests
// ============================================================================

TEST(PacketTest, TruncatedLengthPrefixes) {
    // 0 bytes: cannot even read key length prefix
    {
        Packet empty_packet(std::vector<std::byte>{});
        EXPECT_FALSE(empty_packet.deserialize_data().has_value());
    }

    // 1 to 3 bytes: partial key length prefix
    for (std::size_t size = 1; size < sizeof(std::uint32_t); ++size) {
        std::vector<std::byte> partial(size, std::byte{0x01});
        Packet packet(std::move(partial));
        EXPECT_FALSE(packet.deserialize_data().has_value());
    }
}

TEST(PacketTest, TruncatedKeyPayload) {
    Packet valid;
    valid.serialise_data(to_bytes("hello_world"), to_bytes("val"));

    // Cut off bytes in the middle of the key
    const auto payload = valid.payload();
    std::vector<std::byte> truncated(payload.begin(), payload.begin() + 8); // only 4 bytes key
    Packet packet(std::move(truncated));

    EXPECT_FALSE(packet.deserialize_data().has_value());
}

TEST(PacketTest, TruncatedValueHeader) {
    Packet valid;
    valid.serialise_data(to_bytes("key"), to_bytes("val"));

    // Key has 4 (len) + 3 (data) = 7 bytes.
    // Cut off right after key data, so value length header is missing / partial.
    const auto payload = valid.payload();
    std::vector<std::byte> truncated(payload.begin(), payload.begin() + 8); // only 1 byte of value len header
    Packet packet(std::move(truncated));

    EXPECT_FALSE(packet.deserialize_data().has_value());
}

TEST(PacketTest, TruncatedValuePayload) {
    Packet valid;
    valid.serialise_data(to_bytes("key"), to_bytes("longer_value_data"));

    // Remove the last 3 bytes of the value payload
    const auto payload = valid.payload();
    std::vector<std::byte> truncated(payload.begin(), payload.end() - 3);
    Packet packet(std::move(truncated));

    EXPECT_FALSE(packet.deserialize_data().has_value());
}

TEST(PacketTest, CorruptMassiveLengthPrefixes) {
    // 1. Massive key length that exceeds buffer
    {
        std::vector<std::byte> buffer(16, std::byte{0});
        std::uint32_t massive_len = to_little_endian_32(0xFFFFFFFFU);
        std::memcpy(buffer.data(), &massive_len, sizeof(massive_len));

        Packet packet(std::move(buffer));
        EXPECT_FALSE(packet.deserialize_data().has_value());
    }

    // 2. Valid key, but massive value length
    {
        Packet valid;
        valid.serialise_data(to_bytes("k"), to_bytes("v"));
        std::vector<std::byte> buffer(valid.payload().begin(), valid.payload().end());

        // Offset to value_length is 4 (key_len) + 1 (key data) = 5
        std::uint32_t massive_val_len = to_little_endian_32(0xFFFFFFFFU);
        std::memcpy(buffer.data() + 5, &massive_val_len, sizeof(massive_val_len));

        Packet packet(std::move(buffer));
        EXPECT_FALSE(packet.deserialize_data().has_value());
    }
}

// ============================================================================
// Move Semantics & Cloning Tests
// ============================================================================

TEST(PacketTest, MoveAndCloneSemantics) {
    Packet original;
    original.serialise_data(to_bytes("move_key"), to_bytes("move_val"));

    // 1. Test clone
    Packet cloned = original.clone();
    EXPECT_EQ(cloned.size(), original.size());
    auto cloned_res = cloned.deserialize_data();
    ASSERT_TRUE(cloned_res.has_value());
    EXPECT_EQ(to_string_view(std::get<0>(*cloned_res)), "move_key");

    // 2. Test move construction
    Packet moved(std::move(original));
    auto moved_res = moved.deserialize_data();
    ASSERT_TRUE(moved_res.has_value());
    EXPECT_EQ(to_string_view(std::get<0>(*moved_res)), "move_key");
    EXPECT_EQ(to_string_view(std::get<1>(*moved_res)), "move_val");
}
