#include "client.hpp"

#include <iostream>
#include <string>

int main(int argc, const char **argv) {
  const std::string port = (argc >= 2) ? argv[1] : "50051";
  const std::string name = (argc >= 3) ? argv[2] : "World";
  const std::string host = "localhost:" + port;

  GreeterClient client{host};

  std::string reply;
  client.test_client_mr();

  return 0;
}
