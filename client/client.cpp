#include "client.hpp"
#include "helper.hpp"
#include "mapreduce.grpc.pb.h"
#include "mapreduce.pb.h"

#include <agrpc/client_rpc.hpp>
#include <agrpc/run.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <chrono>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <grpcpp/support/status.h>
#include <iostream>
#include <ostream>
#include <string>

GreeterClient::GreeterClient(const std::string &host)
    : host_(host),
      stub_(grpc::CreateChannel(host, grpc::InsecureChannelCredentials())),
      mr_stub_(mr_proto::MapReduceService::NewStub(
          grpc::CreateChannel(host, grpc::InsecureChannelCredentials()))) {}

GreeterClient::~GreeterClient() = default;

boost::asio::awaitable<bool>
GreeterClient::say_hello_async(const std::string &name,
                               std::string &reply_message,
                               std::chrono::milliseconds timeout) {
  using RPC =
      agrpc::ClientRPC<&helloworld::Greeter::Stub::PrepareAsyncSayHello>;

  grpc::ClientContext client_context;
  client_context.set_deadline(std::chrono::system_clock::now() + timeout);

  helloworld::HelloRequest request;
  request.set_name(name);

  helloworld::HelloReply response;
  std::cout << "[Client] Sending SayHello request for: '" << name << "' to "
            << host_ << std::endl;

  grpc::Status status = co_await RPC::request(
      grpc_context_, stub_, client_context, request, response);

  if (status.ok()) {
    reply_message = response.message();
    std::cout << "[Client] Received response: " << reply_message << std::endl;
    co_return true;
  }

  std::cerr << "[Client] RPC failed (Code " << status.error_code()
            << "): " << status.error_message() << std::endl;
  co_return false;
}

auto GreeterClient::poll_task_async(mr_proto::PollRequest &request,
                                    mr_proto::TaskAssignment &response,
                                    std::chrono::milliseconds timeout)
    -> boost::asio::awaitable<bool> {

  using RPC =
      agrpc::ClientRPC<&mr_proto::MapReduceService::Stub::PrepareAsyncPollTask>;

  grpc::ClientContext client_context_;
  client_context_.set_deadline(std::chrono::system_clock::now() + timeout);

  grpc::Status status = co_await RPC::request(
      grpc_context_, *mr_stub_, client_context_, request, response);

  if (status.ok()) {
    co_return true;
  }
  std::cerr << "[Client] PollTask failed: " << status.error_message()
            << std::endl;
  co_return false;
}

bool GreeterClient::say_hello(const std::string &name,
                              std::string &reply_message,
                              std::chrono::milliseconds timeout) {
  bool success = false;
  grpc_context_.reset();

  boost::asio::co_spawn(
      grpc_context_,
      [&]() -> boost::asio::awaitable<void> {
        success = co_await say_hello_async(name, reply_message, timeout);
      },
      helper::RethrowFirstArg{});

  grpc_context_.run();
  return success;
}

auto GreeterClient::poll_task_loop() -> boost::asio::awaitable<void> {

  for (int i = 0;; i++) {
    mr_proto::PollRequest request;
    request.set_worker_id(std::to_string(i));

    mr_proto::TaskAssignment response;
    bool ok = co_await poll_task_async(request, response);

    if (!ok)
      break;

    if (response.type() == mr_proto::TaskType::SHUTDOWN) {
      break;
    }

    if (response.type() == mr_proto::TaskType::WAIT) {

      std::cout << "[CLient] : waiting in polling" << std::endl;

      auto executor = co_await boost::asio::this_coro::executor;

      boost::asio::steady_timer timer(executor);
      timer.expires_after(std::chrono::milliseconds(500));
      co_await timer.async_wait(boost::asio::use_awaitable);
    }
  }
}

auto GreeterClient::handle_complete_task(mr_proto::CompleteRequest &request,
                                         mr_proto::CompleteReply &response,
                                         std::chrono::milliseconds timeout)
    -> boost::asio::awaitable<void> {

  using RPC = agrpc::ClientRPC<
      &mr_proto::MapReduceService::Stub::PrepareAsyncCompleteTask>;

  grpc::ClientContext client_context_;
  client_context_.set_deadline(std::chrono::system_clock::now() + timeout);

  grpc::Status status = co_await RPC::request(
      grpc_context_, *mr_stub_, client_context_, request, response);

  if (!status.ok()) {
    std::cerr << "[Client] CompleteTask RPC failed: " << status.error_message()
              << std::endl;
    co_return;
  }

  if (response.accepted()) {
    std::cout << "[Client] Task commit accepted by Coordinator." << std::endl;
  } else {
    std::cerr << "[Client] Task commit rejected (Fenced! Lease expired)."
              << std::endl;
  }
}

auto GreeterClient::handle_report_shuffle_error(
    mr_proto::ShuffleErrorReport &request,
    mr_proto::ShuffleErrorReply &response, std::chrono::milliseconds timeout)
    -> boost::asio::awaitable<void> {
  using RPC = agrpc::ClientRPC<
      &mr_proto::MapReduceService::Stub::PrepareAsyncReportShuffleError>;

  grpc::ClientContext client_context_;
  client_context_.set_deadline(std::chrono::system_clock::now() + timeout);

  grpc::Status status = co_await RPC::request(
      grpc_context_, *mr_stub_, client_context_, request, response);

  if (!status.ok()) {
    std::cerr << "[Client] ReportShuffleError RPC failed: "
              << status.error_message() << std::endl;
    co_return;
  }
}

void GreeterClient::test_client_mr() {
  /**
    bool success = false;
  grpc_context_.reset();

  boost::asio::co_spawn(
      grpc_context_,
      [&]() -> boost::asio::awaitable<void> {
        success = co_await say_hello_async(name, reply_message, timeout);
      },
      helper::RethrowFirstArg{});

  grpc_context_.run();
  return success;
  */

  grpc_context_.reset();
  io_context_.restart();

  boost::asio::co_spawn(
      grpc_context_,
      [&]() -> boost::asio::awaitable<void> { co_await poll_task_loop(); },
      helper::RethrowFirstArg{});

  boost::asio::co_spawn(
      grpc_context_,
      [&]() -> boost::asio::awaitable<void> {
        mr_proto::CompleteRequest request;
        request.set_generation_id(1);
        std::vector<std::string> produced_files = {"mr_output/mr-0_0.data",
                                                   "mr_output/mr-0_1.data",
                                                   "mr_output/mr-0_2.data"};
        for (const auto &file : produced_files) {
          request.add_file_paths(file);
        }

        mr_proto::CompleteReply response;
        co_await handle_complete_task(request, response);
      },
      helper::RethrowFirstArg{});

  boost::asio::co_spawn(
      grpc_context_,
      [&]() -> boost::asio::awaitable<void> {
        mr_proto::ShuffleErrorReport request;
        request.set_generation_id(1);
        request.set_error_message("testing");
        request.set_missing_path("testing.tmp");

        mr_proto::ShuffleErrorReply response;

        co_await handle_report_shuffle_error(request, response);
      },
      helper::RethrowFirstArg{});

  agrpc::run(grpc_context_, io_context_);
}
