#pragma once
#include <concepts>
#include <cstddef>
#include <functional>
#include <string_view>
namespace mr {

template <typename T>
concept IsPartitioner =
    requires(const T &obj, std::string_view key, std::size_t num_partitions) {
      { obj.partition(key, num_partitions) } -> std::same_as<std::size_t>;
    };

// template <typename T> struct PartitionerInterface {
//   // Delay the concept check until the derived class is fully defined
//   ~PartitionerInterface() {
//     static_assert(IsPartitioner<T>, "partitioner interface is not
//     implemented");
//   }
// };

// class HashPartitioner : public PartitionerInterface<HashPartitioner> {
// public:
//   auto partition(std::string_view key, std::size_t num_partitions) const
//       -> std::size_t {
//     return std::hash<std::string_view>{}(key) % num_partitions;
//   }
// };

class HashPartitioner {
public:
  auto partition(std::string_view key, std::size_t num_partitions) const
      -> std::size_t {
    return std::hash<std::string_view>{}(key) % num_partitions;
  }
};

static_assert(IsPartitioner<HashPartitioner>);

} // namespace mr