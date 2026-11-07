#pragma once

#include "mr_coordination.grpc.pb.h"
#include "mr_coordination.pb.h"
#include "server_shutdown.hpp"
#include "shuffle.grpc.pb.h"
#include "state.hpp"
#include <agrpc/grpc_context.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>
#include <cstddef>
#include <expected>
#include <grpcpp/support/status.h>
#include <memory>
#include <thread>
namespace mr {
class WorkerServer : public std::enable_shared_from_this<WorkerServer> {
public:
  // worker client to coordinate server stub

  auto poll_task_async(wc::PollRequest &request) -> boost::asio::awaitable<
      std::expected<wc::TaskAssignment, grpc::Status>>;

  auto poll_heart_beat(wc::HeartBeatRequest &request) -> boost::asio::awaitable<
      std::expected<wc::HeartBeatReply, grpc::Status>>;

  auto fetch_map_data(wc::SplitDataRequest &request)
      -> boost::asio::awaitable<grpc::Status>;

private:
  explicit WorkerServer(std::string host, std::size_t num_threads);

  /*
   static std::shared_ptr<CoordinateServer>
  create(std::string host, std::size_t num_threads,
         std::shared_ptr<CoordinatorState> state) {
    auto server = std::shared_ptr<CoordinateServer>(
        new CoordinateServer(std::move(host), num_threads, std::move(state)));

    // Safely initialize async handlers and timers with shared_from_this()
    server->init();
    return server;
  }
  */

public:
  static auto create(std::string host, std::size_t num_threads)
      -> std::shared_ptr<WorkerServer> {
    auto server = std::shared_ptr<WorkerServer>(
        new WorkerServer(std::move(host), num_threads));
    return server;
  }

  auto start_worker() -> boost::asio::awaitable<void>;
  auto start_heart_beats() -> boost::asio::awaitable<void>;
  void run();

private:
  std::string host_;

  // io related
  boost::asio::io_context io_context_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      main_work_guard_;

  boost::asio::io_context bg_io_context_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      bg_work_guard_;

  // downloading , map and reduce phase work
  std::jthread background_thread_;

  // state of worker
  std::unique_ptr<wc::MapReduceService::Stub> wc_stub_;
  std::unique_ptr<agrpc::GrpcContext> control_grpc_context_;

  // download data
  std::unique_ptr<ww::WorkerShuffleService::Stub> ww_stub_;
  std::unique_ptr<agrpc::GrpcContext> data_grpc_context_;
  ww::WorkerShuffleService::AsyncService data_service_;

  // worker to worker server and client
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<helper::ServerShutdown> server_shutdown_;

  // thread pool to send and receive data and do worker task
  boost::asio::thread_pool thread_pool_;
  WorkerState worker_state_;

  // cancellation signal from heartbeat to other coroutine
  std::shared_ptr<boost::asio::cancellation_signal> signal_;

  // time intervals
  static constexpr std::size_t heart_beat_intervals_in_seconds{2};
};

} // namespace mr