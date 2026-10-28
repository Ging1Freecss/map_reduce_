#include "mr_coordination.grpc.pb.h"
#include "mr_coordination.pb.h"
#include "server_shutdown.hpp"
#include "shuffle.grpc.pb.h"
#include "shuffle.pb.h"
#include <agrpc/grpc_context.hpp>
#include <agrpc/server_rpc.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>
#include <grpcpp/server.h>
#include <memory>
#include <string>

namespace mr {

class CoordinateServer {
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

public:
  explicit CoordinateServer(std::string host);
  ~CoordinateServer();

  auto handle_poll_task(PollTaskRPC &rpc, wc::PollRequest &request)
      -> boost::asio::awaitable<void>;

  auto handle_heart_beat(HeartBeatRPC &rpc, wc::HeartBeatRequest &request)
      -> boost::asio::awaitable<void>;

  auto handle_complete_task(CompleteTaskRPC &rpc, wc::CompleteRequest &request)
      -> boost::asio::awaitable<void>;

  auto handle_report_shuffle_error(ReportShuffleErrorRPC &rpc,
                                   wc::ShuffleErrorReport &request)
      -> boost::asio::awaitable<void>;

  void register_handlers();
  void run();
  void stop();

private:
  std::string host_;

  wc::MapReduceService::AsyncService wc_service_;
  std::unique_ptr<boost::asio::io_context> io_context_;
  std::unique_ptr<agrpc::GrpcContext> grpc_context_;
  std::unique_ptr<grpc::Server> server_;

  boost::asio::thread_pool thread_pool_;

  std::unique_ptr<helper::ServerShutdown> server_shutdown_;
};

} // namespace mr