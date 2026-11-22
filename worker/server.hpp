#pragma once

#include "mr_coordination.grpc.pb.h"
#include "mr_coordination.pb.h"
#include "server_shutdown.hpp"
#include "shuffle.grpc.pb.h"
#include "shuffle.pb.h"
#include "state.hpp"
#include <agrpc/detail/forward.hpp>
#include <agrpc/grpc_context.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>
#include <cstddef>
#include <expected>
#include <grpcpp/support/status.h>
#include <memory>
#include <optional>
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

  auto fetch_input_data_for_reduce(const std::string &peer_endpoint,
                                   const std::string &partition_filename)
      -> boost::asio::awaitable<grpc::Status>;

  auto fetch_all_input_for_reduce() -> boost::asio::awaitable<
      std::optional<std::vector<MapPhaseOutputFilesStatus>>>;

  auto poll_complete_task(wc::CompleteRequest &request)
      -> boost::asio::awaitable<std::expected<wc::CompleteReply, grpc::Status>>;

  // fix shuffle error phase
  auto poll_report_shuffle_error(wc::ShuffleErrorReport &request)
      -> boost::asio::awaitable<
          std::expected<wc::ShuffleErrorReply, grpc::Status>>;

  auto poll_shuffle_error_fix(wc::ShuffleFixRequest &request)
      -> boost::asio::awaitable<
          std::expected<wc::TaskAssignment, grpc::Status>>;

  auto shuffle_phase_loop(std::vector<MapPhaseOutputFilesStatus> &file_data)
      -> boost::asio::awaitable<
          std::optional<std::vector<std::filesystem::path>>>;
  // --- end ---

  using FetchPartitionRPC = agrpc::ServerRPC<
      &ww::WorkerShuffleService::AsyncService::RequestFetchPartition>;
  auto handle_fetch_partition(FetchPartitionRPC &rpc,
                              ww::PartitionRequest &request)
      -> boost::asio::awaitable<void>;

  void register_handlers();

  auto process_map_task(wc::TaskAssignment task)
      -> boost::asio::awaitable<void>;

  auto process_reduce_task(wc::TaskAssignment task)
      -> boost::asio::awaitable<void>;

private:
  explicit WorkerServer(std::string coordinator_endpoint,
                        std::string listen_address, std::string advertised_host,
                        std::size_t num_threads);

public:
  static auto create(std::string coordinator_endpoint,
                     std::string listen_address, std::string advertised_host,
                     std::size_t num_threads = 4)
      -> std::shared_ptr<WorkerServer> {
    auto server = std::shared_ptr<WorkerServer>(new WorkerServer(
        std::move(coordinator_endpoint), std::move(listen_address),
        std::move(advertised_host), num_threads));

    server->register_handlers();
    return server;
  }

  auto start_worker() -> boost::asio::awaitable<void>;
  auto start_heart_beats() -> boost::asio::awaitable<void>;
  void run();
  auto run_worker_with_heartbeats() -> boost::asio::awaitable<void>;
  ~WorkerServer();

private:
  std::string coordinator_endpoint_;
  std::string listen_address_;
  std::string advertised_host_;
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
  std::unique_ptr<wc::MapReduceService::Stub> wc_control_stub_;
  std::unique_ptr<wc::MapReduceService::Stub> wc_data_stub_;
  std::unique_ptr<agrpc::GrpcContext> control_grpc_context_;

  // download data
  std::unique_ptr<agrpc::GrpcContext> data_grpc_context_;
  ww::WorkerShuffleService::AsyncService data_shuffle_service_;

  // worker to worker server and client
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<helper::ServerShutdown> server_shutdown_;

  // thread pool to send and receive data and do worker task
  boost::asio::thread_pool thread_pool_;
  WorkerState worker_state_;

  // cancellation signal from heartbeat to other coroutine
  std::shared_ptr<boost::asio::cancellation_signal> signal_;

  // time intervals
  static constexpr std::chrono::milliseconds heart_beat_interval{2000};
  static constexpr std::chrono::milliseconds poll_task_wait_interval{500};
};

} // namespace mr