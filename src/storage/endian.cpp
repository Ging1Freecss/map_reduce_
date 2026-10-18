#include "storage/endian.hpp"

Packet::Packet(std::vector<std::byte> &&payload)
    : payload_(std::move(payload)) {}

Packet::Packet(std::span<const std::byte> payload)
    : payload_(payload.begin(), payload.end()) {}

// key,value given serialised them and store in payload_
void Packet::serialise_data(std::span<const std::byte> key,
                            std::span<const std::byte> value) {

  std::uint32_t key_size = key.size();
  std::uint32_t value_size = value.size();

  // store length of key data + key data + lenth of value data + value data
  std::uint32_t total_size =
      sizeof(std::uint32_t) + key_size + sizeof(std::uint32_t) + value_size;

  std::uint32_t little_endian_key_size = to_little_endian_32(key_size);
  std::uint32_t little_endian_value_size = to_little_endian_32(value_size);

  payload_.resize(total_size);
  std::byte *ptr = payload_.data();

  // for key size
  std::memcpy(ptr, &little_endian_key_size, sizeof(little_endian_key_size));
  ptr += sizeof(little_endian_key_size);

  // for key data
  if (!key.empty()) {
    std::memcpy(ptr, key.data(), key_size);
    ptr += key_size;
  }

  // for value size
  std::memcpy(ptr, &little_endian_value_size, sizeof(little_endian_value_size));
  ptr += sizeof(little_endian_value_size);

  // value data
  if (!value.empty()) {
    std::memcpy(ptr, value.data(), value_size);
    ptr += value_size;
  }

  return;
}

auto Packet::deserialize_data() const -> std::optional<
    std::tuple<std::span<const std::byte>, std::span<const std::byte>>> {

  // invalid data , if can't get first 4 bytes
  if (payload_.size() < sizeof(std::uint32_t)) {
    return std::nullopt;
  }
  std::size_t offset{0};

  std::uint32_t endian_key_length{0};
  std::memcpy(&endian_key_length, payload_.data(), sizeof(endian_key_length));

  offset += sizeof(endian_key_length); // start of key data
  std::uint32_t key_length = from_little_endian_32(endian_key_length);

  // check if value data exist
  // payload.size - sizeof key_length variable - key_length - sizeof
  // value_length variable > 0,  we rearrange the var for not triggering
  // buffer overflow

  if (payload_.size() - sizeof(std::uint32_t) <
      sizeof(std::uint32_t) + key_length) {
    return std::nullopt;
  }

  std::span<const std::byte> key_span{payload_.data() + offset, key_length};
  offset += key_length; // start of value length data

  std::uint32_t endian_value_length{0};
  std::memcpy(&endian_value_length, payload_.data() + offset,
              sizeof(std::uint32_t));

  std::uint32_t value_length = from_little_endian_32(endian_value_length);

  // to check value data exist
  if (payload_.size() < offset + sizeof(endian_value_length) + value_length) {
    return std::nullopt;
  }

  offset += sizeof(endian_value_length); // start of value data

  std::span<const std::byte> value_span{payload_.data() + offset, value_length};

  return std::tuple{key_span, value_span};
};

auto Packet::payload() const noexcept -> std::span<const std::byte> {
  return std::span<const std::byte>{payload_};
}

auto Packet::size() const noexcept -> std::size_t { return payload_.size(); }

auto Packet::clone() const -> Packet { return Packet(payload_); }