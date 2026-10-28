#pragma once

#include "helloworld.grpc.pb.h"
#include "helper.hpp"
#include "mapreduce.grpc.pb.h"
#include "mapreduce.pb.h"

#include <agrpc/grpc_context.hpp>
#include <agrpc/grpc_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
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

  auto
  poll_task_async(mr_proto::PollRequest &request,
                  mr_proto::TaskAssignment &response,
                  std::chrono::milliseconds timeout = std::chrono::seconds(5))
      -> boost::asio::awaitable<bool>;

  auto poll_task_loop() -> boost::asio::awaitable<void>;

  auto handle_complete_task(
      mr_proto::CompleteRequest &request, mr_proto::CompleteReply &response,
      std::chrono::milliseconds timeout = std::chrono::seconds(5))
      -> boost::asio::awaitable<void>;

  auto handle_report_shuffle_error(
      mr_proto::ShuffleErrorReport &request,
      mr_proto::ShuffleErrorReply &response,
      std::chrono::milliseconds timeout = std::chrono::seconds(5))
      -> boost::asio::awaitable<void>;

  bool say_hello(const std::string &name, std::string &reply_message,
                 std::chrono::milliseconds timeout = std::chrono::seconds(5));

  agrpc::GrpcContext &grpc_context() { return grpc_context_; }
  boost::asio::io_context &io_context() { return io_context_; }
  helloworld::Greeter::Stub &stub() { return stub_; }
  mr_proto::MapReduceService::Stub &mr_stub() { return *mr_stub_; }

  void test_client_mr();

private:
  std::string host_;
  helloworld::Greeter::Stub stub_;
  std::unique_ptr<mr_proto::MapReduceService::Stub> mr_stub_;
  boost::asio::io_context io_context_;
  agrpc::GrpcContext grpc_context_;
};
