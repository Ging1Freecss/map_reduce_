#pragma once

#include "helloworld.grpc.pb.h"
#include "helper.hpp"

#include <agrpc/grpc_context.hpp>
#include <agrpc/grpc_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <grpcpp/channel.h>

#include <chrono>
#include <memory>
#include <string>

/**
 * @brief GreeterClient manages the client-side channel, stub, and asynchronous
 *        execution for communicating with the gRPC Greeter service.
 */
class GreeterClient {
public:
  explicit GreeterClient(const std::string &host);
  ~GreeterClient();

  boost::asio::awaitable<bool>
  say_hello_async(const std::string &name, std::string &reply_message,
                  std::chrono::milliseconds timeout = std::chrono::seconds(5));

  bool say_hello(const std::string &name, std::string &reply_message,
                 std::chrono::milliseconds timeout = std::chrono::seconds(5));

  agrpc::GrpcContext &grpc_context() { return grpc_context_; }
  helloworld::Greeter::Stub &stub() { return stub_; }

private:
  std::string host_;
  helloworld::Greeter::Stub stub_;
  agrpc::GrpcContext grpc_context_;
};
