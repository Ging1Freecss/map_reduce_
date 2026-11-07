#include "mr_coordination.grpc.pb.h"
#include "mr_coordination.pb.h"
#include "server_shutdown.hpp"
#include "state.hpp"
#include <agrpc/detail/forward.hpp>
#include <agrpc/grpc_context.hpp>
#include <agrpc/server_rpc.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/thread_pool.hpp>
#include <grpcpp/server.h>
#include <memory>
#include <string>

namespace mr {

class CoordinateServer : public std::enable_shared_from_this<CoordinateServer> {
public:
  using PollTaskRPC =
      agrpc::ServerRPC<&wc::MapReduceService::AsyncService::RequestPollTask>;

  using HeartBeatRPC =
      agrpc::ServerRPC<&wc::MapReduceService::AsyncService::RequestHeartbeat>;

  using CompleteTaskRPC = agrpc::ServerRPC<
      &wc::MapReduceService::AsyncService::RequestCompleteTask>;

  using ReportShuffleErrorRPC = agrpc::ServerRPC<
      &wc::MapReduceService::AsyncService::RequestReportShuffleError>;

  using FetchInputSplitRPC = agrpc::ServerRPC<
      &wc::MapReduceService::AsyncService::RequestFetchInputSplit>;

  using ShuffleErrorFixRPC = agrpc::ServerRPC<
      &wc::MapReduceService::AsyncService::RequestShuffleErrorFix>;

private:
  explicit CoordinateServer(std::string host, std::size_t num_thread_,
                            std::shared_ptr<CoordinatorState> coordinate_state);

public:
  ~CoordinateServer();

  static std::shared_ptr<CoordinateServer>
  create(std::string host, std::size_t num_threads,
         std::shared_ptr<CoordinatorState> state) {
    auto server = std::shared_ptr<CoordinateServer>(
        new CoordinateServer(std::move(host), num_threads, std::move(state)));

    // Safely initialize async handlers and timers with shared_from_this()
    server->init();
    return server;
  }

  void init();

  auto handle_poll_task(PollTaskRPC &rpc, wc::PollRequest &request)
      -> boost::asio::awaitable<void>;

  auto handle_heart_beat(HeartBeatRPC &rpc, wc::HeartBeatRequest &request)
      -> boost::asio::awaitable<void>;

  auto handle_complete_task(CompleteTaskRPC &rpc, wc::CompleteRequest &request)
      -> boost::asio::awaitable<void>;

  auto handle_report_shuffle_error(ReportShuffleErrorRPC &rpc,
                                   wc::ShuffleErrorReport &request)
      -> boost::asio::awaitable<void>;

  auto scheduleRecurringTasks() -> boost::asio::awaitable<void>;

  auto handle_fetch_input_split(FetchInputSplitRPC &rpc,
                                wc::SplitDataRequest &request)
      -> boost::asio::awaitable<void>;
  /*
   // Input Data Streaming (Coordinator -> Map Worker)
rpc FetchInputSplit(SplitDataRequest) returns (stream SplitDataChunk);

// Targeted re-attachment for the waiting Reducer
rpc ShuffleErrorFix(ShuffleFixRequest) returns (TaskAssignment);
  */

  void register_handlers();
  void run();
  void stop();

private:
  std::string host_;

  wc::MapReduceService::AsyncService wc_service_;

  // io context and control grpc context run in same thread
  // using agrpc::run()
  boost::asio::io_context io_context_;
  std::unique_ptr<agrpc::GrpcContext> control_grpc_context_;

  // dedicated for data stream
  std::unique_ptr<agrpc::GrpcContext> data_grpc_context_;
  boost::asio::thread_pool thread_pool_;

  // run to check heartbeats
  boost::asio::steady_timer timer_;

  std::unique_ptr<grpc::Server> server_;

  std::unique_ptr<helper::ServerShutdown> server_shutdown_;

  //  hold state of current data handle by coordinator
  std::shared_ptr<CoordinatorState> coordinate_state_;
};

} // namespace mr