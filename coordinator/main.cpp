#include "server.hpp"
#include <exception>
#include <iostream>
#include <string>

int main(int argc, const char **argv) {
  const std::string port = (argc >= 2) ? argv[1] : "50051";
  const std::string host = "0.0.0.0:" + port;

  try {
  
  } catch (const std::exception &e) {
    std::cerr << "Server fatal error: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
