#pragma once

#include "helloworld.grpc.pb.h"
#include "helper.hpp"
#include "server_shutdown.hpp"

#include <agrpc/grpc_context.hpp>
#include <agrpc/server_rpc.hpp>
#include <boost/asio/awaitable.hpp>
#include <grpcpp/server.h>

#include <memory>
#include <string>

/**
 * @brief GreeterServer encapsulates the lifecycle and RPC request handling
 *        for the gRPC Greeter service using Boost.Asio and C++20 coroutines.
 */
class GreeterServer
{
public:
    using SayHelloRPC = agrpc::ServerRPC<&helloworld::Greeter::AsyncService::RequestSayHello>;

    explicit GreeterServer(std::string host);
    ~GreeterServer();

    void run();
    void stop();

private:
    void register_handlers();

    boost::asio::awaitable<void> handle_say_hello(
        SayHelloRPC& rpc,
        helloworld::HelloRequest& request);

    std::string host_;
    helloworld::Greeter::AsyncService service_;
    std::unique_ptr<agrpc::GrpcContext> grpc_context_;
    std::unique_ptr<grpc::Server> server_;
    std::unique_ptr<helper::ServerShutdown> server_shutdown_;
};
