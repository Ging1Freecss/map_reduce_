#include "server.hpp"
#include "state.hpp"
#include <exception>
#include <iostream>
#include <string>

int main(int argc, const char **argv) {
  std::filesystem::path input_file;
  std::string port = "50051";
  std::size_t num_reducers = 4;
  std::size_t num_threads = 4;
  std::uint64_t target_split_bytes = 64 * 1024 * 1024;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if ((arg == "-p" || arg == "--port") && i + 1 < argc) {
      port = argv[++i];
    } else if ((arg == "-r" || arg == "--reducers") && i + 1 < argc) {
      num_reducers = std::stoul(argv[++i]);
    } else if ((arg == "-t" || arg == "--threads") && i + 1 < argc) {
      num_threads = std::stoul(argv[++i]);
    } else if ((arg == "-s" || arg == "--split-size") && i + 1 < argc) {
      target_split_bytes = std::stoull(argv[++i]);
    } else if (!arg.starts_with("-")) {
      input_file = arg;
    }
  }
  if (input_file.empty() || !std::filesystem::exists(input_file)) {
    std::cerr << "Usage: coordinator <input_file> [-p port] [-r reducers] [-t "
                 "threads] [-s split_size_bytes]\n";
    return 1;
  }
  const std::string host = "0.0.0.0:" + port;
  std::cout << "[Coordinator] Starting on " << host
            << " with input=" << input_file << ", reducers=" << num_reducers
            << ", split_bytes=" << target_split_bytes << "\n";
  try {
    // 1. Create State machine (splits the input file and creates map/reduce
    // tasks)
    auto state = mr::CoordinatorState::create(input_file, num_reducers,
                                              target_split_bytes);
    // 2. Create and run server
    auto server = mr::CoordinateServer::create(host, num_threads, state);
    server->run();
  } catch (const std::exception &e) {
    std::cerr << "[Coordinator] Fatal error: " << e.what() << "\n";
    return 1;
  }
  return 0;
}
