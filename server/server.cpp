#include "server.hpp"
#include "helper.hpp"
#include "mapreduce.pb.h"

#include <agrpc/register_awaitable_rpc_handler.hpp>
#include <agrpc/run.hpp>
#include <boost/asio/awaitable.hpp>
#include <grpcpp/server_builder.h>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <utility>

GreeterServer::GreeterServer(std::string host) : host_(std::move(host)) {
  grpc::ServerBuilder builder;
  grpc_context_ =
      std::make_unique<agrpc::GrpcContext>(builder.AddCompletionQueue());
  builder.AddListeningPort(host_, grpc::InsecureServerCredentials());
  builder.RegisterService(&service_);
  builder.RegisterService(&mr_service_);

  server_ = builder.BuildAndStart();
  if (!server_) {
    throw std::runtime_error("Failed to start gRPC server on " + host_);
  }

  server_shutdown_ =
      std::make_unique<helper::ServerShutdown>(*server_, *grpc_context_, &io_context_);
  register_handlers();
}

GreeterServer::~GreeterServer() = default;

void GreeterServer::register_handlers() {
  agrpc::register_awaitable_rpc_handler<SayHelloRPC>(
      *grpc_context_, service_,
      [this](SayHelloRPC &rpc, helloworld::HelloRequest &request)
          -> boost::asio::awaitable<void> {
        co_return co_await handle_say_hello(rpc, request);
      },
      helper::RethrowFirstArg{});

  agrpc::register_awaitable_rpc_handler<PollTaskRPC>(
      *grpc_context_, mr_service_,
      [this](PollTaskRPC &rpc,
             mr_proto::PollRequest &request) -> boost::asio::awaitable<void> {
        co_await handle_poll_task(rpc, request);
        co_return;
      },
      helper::RethrowFirstArg{});

  agrpc::register_awaitable_rpc_handler<CompleteTaskRPC>(
      *grpc_context_, mr_service_,
      [this](CompleteTaskRPC &rpc, mr_proto::CompleteRequest &request)
          -> boost::asio::awaitable<void> {
        co_await handle_complete_task(rpc, request);
        co_return;
      },
      helper::RethrowFirstArg{});

  agrpc::register_awaitable_rpc_handler<ReportShuffleErrorRPC>(
      *grpc_context_, mr_service_,
      [this](ReportShuffleErrorRPC &rpc, mr_proto::ShuffleErrorReport &request)
          -> boost::asio::awaitable<void> {
        co_await handle_report_shuffle_error(rpc, request);
        co_return;
      },
      helper::RethrowFirstArg{});
}

boost::asio::awaitable<void>
GreeterServer::handle_say_hello(SayHelloRPC &rpc,
                                helloworld::HelloRequest &request) {
  std::cout << "[Server] Handling SayHello request for: " << request.name()
            << std::endl;

  helloworld::HelloReply response;
  response.set_message("Hello " + request.name() +
                       " from Boost.Asio gRPC server!");

  co_await rpc.finish(response, grpc::Status::OK);
}

auto GreeterServer::handle_poll_task(PollTaskRPC &rpc,
                                     mr_proto::PollRequest &request)
    -> boost::asio::awaitable<void> {
  std::string worker_id = request.worker_id();
  std::cout << "[Server] worker id : " << worker_id << std::endl;
  mr_proto::TaskAssignment task;

  if (worker_id == "3") {
    task.set_type(mr_proto::TaskType::SHUTDOWN);
  } else {
    task.set_type(mr_proto::TaskType::WAIT);
  }

  co_await rpc.finish(task, grpc::Status::OK);
}

auto GreeterServer::handle_complete_task(CompleteTaskRPC &rpc,
                                         mr_proto::CompleteRequest &request)
    -> boost::asio::awaitable<void> {
  std::cout << "[Server] : ";
  for (const auto &s : request.file_paths()) {
    std::cout << s << " ";
  }
  std::cout << std::endl;
  std::cout << "[Server] :" << request.generation_id() << std::endl;

  mr_proto::CompleteReply reply;
  reply.set_accepted(true);

  co_await rpc.finish(reply, grpc::Status::OK);
}

auto GreeterServer::handle_report_shuffle_error(
    ReportShuffleErrorRPC &rpc, mr_proto::ShuffleErrorReport &request)
    -> boost::asio::awaitable<void> {
  std::cout << "[Server] : " << "generation id :" << request.generation_id()
            << std::endl;

  std::cout << "[Server] : " << "missing_path : " << request.missing_path()
            << std::endl;

  std::cout << "[Server] : " << "error_message : " << request.error_message()
            << std::endl;

  mr_proto::ShuffleErrorReply reply;
  reply.set_acknowledged(true);

  co_await rpc.finish(reply, grpc::Status::OK);
}
void GreeterServer::run() {
  std::cout << "Server listening on " << host_ << std::endl;
  agrpc::run(*grpc_context_, io_context_);
  std::cout << "Server stopped cleanly." << std::endl;
}

void GreeterServer::stop() {
  if (server_shutdown_) {
    server_shutdown_->shutdown();
  }
  io_context_.stop();
}
