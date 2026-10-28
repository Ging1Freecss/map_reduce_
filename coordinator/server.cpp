#include "server.hpp"
#include "helper.hpp"
#include "mr_coordination.pb.h"
#include <agrpc/grpc_context.hpp>
#include <agrpc/register_awaitable_rpc_handler.hpp>
#include <agrpc/run.hpp>
#include <boost/asio/awaitable.hpp>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>
#include <memory>

namespace mr {
CoordinateServer::CoordinateServer(std::string host) : host_{host} {

  grpc::ServerBuilder builder_;

  grpc_context_ =
      std::make_unique<agrpc::GrpcContext>(builder_.AddCompletionQueue());
  builder_.AddListeningPort(host, grpc::InsecureServerCredentials());
  builder_.RegisterService(&wc_service_);

  server_ = builder_.BuildAndStart();

  if (!server_) {
    throw std::runtime_error("Failed to start gRPC server on " + host_);
  }

  server_shutdown_ = std::make_unique<helper::ServerShutdown>(
      *server_, *grpc_context_, io_context_.get());
  register_handlers();
}

CoordinateServer::~CoordinateServer() = default;

void CoordinateServer::register_handlers() {
  /**
     agrpc::register_awaitable_rpc_handler<SayHelloRPC>(
    *grpc_context_, service_,
    [this](SayHelloRPC &rpc, helloworld::HelloRequest &request)
        -> boost::asio::awaitable<void> {
      co_return co_await handle_say_hello(rpc, request);
    },
    helper::RethrowFirstArg{});
  */

  agrpc::register_awaitable_rpc_handler<PollTaskRPC>(
      *grpc_context_, wc_service_,
      [this](PollTaskRPC &rpc,
             wc::PollRequest &request) -> boost::asio::awaitable<void> {
        co_await handle_poll_task(rpc, request);
        co_return;
      },
      helper::RethrowFirstArg{});

  agrpc::register_awaitable_rpc_handler<HeartBeatRPC>(
      *grpc_context_, wc_service_,
      [this](HeartBeatRPC &rpc,
             wc::HeartBeatRequest &request) -> boost::asio::awaitable<void> {
        co_await handle_heart_beat(rpc, request);
        co_return;
      },
      helper::RethrowFirstArg{});

  agrpc::register_awaitable_rpc_handler<CompleteTaskRPC>(
      *grpc_context_, wc_service_,
      [this](CompleteTaskRPC &rpc,
             wc::CompleteRequest &request) -> boost::asio::awaitable<void> {
        co_await handle_complete_task(rpc, request);
        co_return;
      },
      helper::RethrowFirstArg{});

  agrpc::register_awaitable_rpc_handler<ReportShuffleErrorRPC>(
      *grpc_context_, wc_service_,
      [this](ReportShuffleErrorRPC &rpc,
             wc::ShuffleErrorReport &request) -> boost::asio::awaitable<void> {
        co_await handle_report_shuffle_error(rpc, request);
        co_return;
      },
      helper::RethrowFirstArg{});
}

auto CoordinateServer::handle_poll_task(PollTaskRPC &rpc,
                                        wc::PollRequest &request)
    -> boost::asio::awaitable<void> {
  wc::TaskAssignment response;
  response.set_type(wc::TaskType::WAIT);
  co_await rpc.finish(response, grpc::Status::OK);
}

void CoordinateServer::run() { agrpc::run(*grpc_context_, *io_context_); }
void CoordinateServer::stop() {
  if (server_shutdown_) {
    server_shutdown_->shutdown();
  }

  io_context_->stop();
}

} // namespace mr