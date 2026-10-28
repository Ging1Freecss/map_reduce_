#pragma once

#include "helloworld.grpc.pb.h"
#include "helper.hpp"
#include "mapreduce.grpc.pb.h"
#include "mapreduce.pb.h"
#include "server_shutdown.hpp"

#include <agrpc/grpc_context.hpp>
#include <agrpc/server_rpc.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <grpcpp/server.h>

#include <memory>
#include <string>

/**
 * @brief GreeterServer encapsulates the lifecycle and RPC request handling
 *        for the gRPC Greeter service using Boost.Asio and C++20 coroutines.
 */
class GreeterServer {
public:
  using SayHelloRPC =
      agrpc::ServerRPC<&helloworld::Greeter::AsyncService::RequestSayHello>;

  using PollTaskRPC = agrpc::ServerRPC<
      &mr_proto::MapReduceService::AsyncService::RequestPollTask>;

  using CompleteTaskRPC = agrpc::ServerRPC<
      &mr_proto::MapReduceService::AsyncService::RequestCompleteTask>;

  using ReportShuffleErrorRPC = agrpc::ServerRPC<
      &mr_proto::MapReduceService::AsyncService::RequestReportShuffleError>;

  explicit GreeterServer(std::string host);
  ~GreeterServer();

  void run();
  void stop();

private:
  void register_handlers();

  boost::asio::awaitable<void>
  handle_say_hello(SayHelloRPC &rpc, helloworld::HelloRequest &request);

  auto handle_poll_task(PollTaskRPC &rpc, mr_proto::PollRequest &request)
      -> boost::asio::awaitable<void>;

  auto handle_complete_task(CompleteTaskRPC &rpc,
                            mr_proto::CompleteRequest &request)
      -> boost::asio::awaitable<void>;

  auto handle_report_shuffle_error(ReportShuffleErrorRPC &rpc,
                                   mr_proto::ShuffleErrorReport &request)
      -> boost::asio::awaitable<void>;

  std::string host_;
  helloworld::Greeter::AsyncService service_;
  mr_proto::MapReduceService::AsyncService mr_service_;
  boost::asio::io_context io_context_;
  std::unique_ptr<agrpc::GrpcContext> grpc_context_;
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<helper::ServerShutdown> server_shutdown_;
};
