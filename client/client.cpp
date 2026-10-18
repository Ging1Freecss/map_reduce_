#include "client.hpp"

#include <agrpc/client_rpc.hpp>
#include <boost/asio/co_spawn.hpp>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <iostream>

GreeterClient::GreeterClient(const std::string& host)
    : host_(host),
      stub_(grpc::CreateChannel(host, grpc::InsecureChannelCredentials()))
{
}

GreeterClient::~GreeterClient() = default;

boost::asio::awaitable<bool> GreeterClient::say_hello_async(
    const std::string& name,
    std::string& reply_message,
    std::chrono::milliseconds timeout)
{
    using RPC = agrpc::ClientRPC<&helloworld::Greeter::Stub::PrepareAsyncSayHello>;

    grpc::ClientContext client_context;
    client_context.set_deadline(std::chrono::system_clock::now() + timeout);

    helloworld::HelloRequest request;
    request.set_name(name);

    helloworld::HelloReply response;
    std::cout << "[Client] Sending SayHello request for: '" << name << "' to " << host_ << std::endl;

    grpc::Status status = co_await RPC::request(grpc_context_, stub_, client_context, request, response);

    if (status.ok())
    {
        reply_message = response.message();
        std::cout << "[Client] Received response: " << reply_message << std::endl;
        co_return true;
    }

    std::cerr << "[Client] RPC failed (Code " << status.error_code()
              << "): " << status.error_message() << std::endl;
    co_return false;
}

bool GreeterClient::say_hello(
    const std::string& name,
    std::string& reply_message,
    std::chrono::milliseconds timeout)
{
    bool success = false;
    grpc_context_.reset();

    boost::asio::co_spawn(
        grpc_context_,
        [&]() -> boost::asio::awaitable<void>
        {
            success = co_await say_hello_async(name, reply_message, timeout);
        },
        helper::RethrowFirstArg{});

    grpc_context_.run();
    return success;
}
