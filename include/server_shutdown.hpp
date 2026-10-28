#pragma once

#include <agrpc/grpc_context.hpp>
#include <agrpc/grpc_executor.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <grpcpp/server.h>

#include <atomic>
#include <iostream>
#include <thread>

namespace helper {
// Helper to cleanly shut down a gRPC server asynchronously without deadlock
struct ServerShutdown {
  ServerShutdown(grpc::Server &server, agrpc::GrpcContext &grpc_context,
                 boost::asio::io_context *io_context = nullptr)
      : server_(server),
        signals_(grpc_context, SIGINT, SIGTERM),
        io_context_(io_context) {
    signals_.async_wait(
        [this](const boost::system::error_code &ec, int /*signal_number*/) {
          if (ec != boost::asio::error::operation_aborted) {
            std::cout << "\nShutdown signal received..." << std::endl;
            shutdown();
          }
        });
  }

  void shutdown() {
    if (!is_shutdown_.exchange(true)) {
      shutdown_thread_ = std::thread([this] {
        signals_.cancel();
        server_.Shutdown();
        if (io_context_) {
          io_context_->stop();
        }
      });
    }
  }

  ~ServerShutdown() {
    if (shutdown_thread_.joinable()) {
      shutdown_thread_.join();
    }
  }

private:
  grpc::Server &server_;
  boost::asio::basic_signal_set<agrpc::GrpcExecutor> signals_;
  boost::asio::io_context *io_context_{nullptr};
  std::atomic<bool> is_shutdown_{false};
  std::thread shutdown_thread_;
};
} // namespace helper
