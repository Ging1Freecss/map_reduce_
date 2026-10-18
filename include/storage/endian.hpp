
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <optional>
#include <span>
#include <vector>

/*
    to maintain big endian across network,
    Big endian : stores MSB at lowest memory address
    Little endian : store LSB at lowest memory address

    if we are operating on a value, doing arithmatic then
    endianess must be maintain
    like key_length is an integer that CPU must intepret as
    a number to do arithmatic

    the cpu copy bytes in a way than maintain orientation of bytes
    from one memory address to another, irrespective of endianess

    so loading a buffer from one place to another does not cause
    issue
*/
// Little-endian framing: 0 CPU overhead on x86_64 / ARM
constexpr auto to_little_endian_32(std::uint32_t val) noexcept
    -> std::uint32_t {
  if constexpr (std::endian::native == std::endian::little) {
    return val;
  } else {
    return std::byteswap(val);
  }
}

constexpr auto from_little_endian_32(std::uint32_t val) noexcept
    -> std::uint32_t {
  return to_little_endian_32(val);
}

class Packet {
private:
  std::vector<std::byte> payload_;

public:
  Packet() = default;

  // for serialised data
  explicit Packet(std::vector<std::byte> &&payload);

  explicit Packet(std::span<const std::byte> payload);

  // key,value given serialised them and store in payload_
  void serialise_data(std::span<const std::byte> key,
                      std::span<const std::byte> value);

  [[nodiscard]]
  auto deserialize_data() const -> std::optional<
      std::tuple<std::span<const std::byte>, std::span<const std::byte>>>;

  [[nodiscard]] auto payload() const noexcept -> std::span<const std::byte>;

  [[nodiscard]]
  auto size() const noexcept -> std::size_t;

  // Move-only semantics
  Packet(const Packet &) = delete;
  Packet &operator=(const Packet &) = delete;

  Packet(Packet &&) noexcept = default;
  Packet &operator=(Packet &&) noexcept = default;

  // Explicit cloning helper if a copy is genuinely necessary
  [[nodiscard]] auto clone() const -> Packet;
};