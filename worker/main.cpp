#include "server.hpp"
#include <iostream>
#include <string>

int main(int argc, const char **argv) {
  std::string coord_endpoint = "127.0.0.1:50051";
  std::string port = "50052";
  std::string host_ip = "127.0.0.1";
  std::size_t num_threads = 4;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if ((arg == "-c" || arg == "--coordinator") && i + 1 < argc) {
      coord_endpoint = argv[++i];
    } else if ((arg == "-p" || arg == "--port") && i + 1 < argc) {
      port = argv[++i];
    } else if ((arg == "-t" || arg == "--threads") && i + 1 < argc) {
      num_threads = std::stoul(argv[++i]);
    } else if ((arg == "-h" || arg == "--host") && i + 1 < argc) {
      host_ip = argv[++i];
    }
  }

  const std::string listen_address = "0.0.0.0:" + port;
  const std::string advertised_host = host_ip + ":" + port;

  std::cout << "[Worker] Coordinator target: " << coord_endpoint << "\n"
            << "[Worker] Listening on:       " << listen_address << "\n"
            << "[Worker] Advertised host:    " << advertised_host << "\n";

  try {
    auto worker = mr::WorkerServer::create(coord_endpoint, listen_address,
                                           advertised_host, num_threads);
    worker->run();
  } catch (const std::exception &e) {
    std::cerr << "[Worker] Fatal error: " << e.what() << "\n";
    return 1;
  }

  return 0;
}