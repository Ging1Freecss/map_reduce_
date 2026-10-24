#include "server.hpp"

#include <agrpc/register_awaitable_rpc_handler.hpp>
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

  server_ = builder.BuildAndStart();
  if (!server_) {
    throw std::runtime_error("Failed to start gRPC server on " + host_);
  }

  server_shutdown_ =
      std::make_unique<helper::ServerShutdown>(*server_, *grpc_context_);
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

void GreeterServer::run() {
  std::cout << "Server listening on " << host_ << std::endl;
  grpc_context_->run();
  std::cout << "Server stopped cleanly." << std::endl;
}

void GreeterServer::stop() {
  if (server_shutdown_) {
    server_shutdown_->shutdown();
  }
}
