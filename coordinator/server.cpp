#include "server.hpp"
#include "helper.hpp"
#include "mr_coordination.pb.h"
#include <agrpc/grpc_context.hpp>
#include <agrpc/register_awaitable_rpc_handler.hpp>
#include <agrpc/run.hpp>
#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <cstddef>
#include <fstream>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>
#include <ios>
#include <memory>

namespace mr {
CoordinateServer::CoordinateServer(
    std::string host, std::size_t num_thread_,
    std::shared_ptr<CoordinatorState> coordinate_state)
    : host_{std::move(host)}, thread_pool_(num_thread_), timer_(io_context_),
      coordinate_state_{std::move(coordinate_state)} {

  grpc::ServerBuilder builder_;

  control_grpc_context_ =
      std::make_unique<agrpc::GrpcContext>(builder_.AddCompletionQueue());
  data_grpc_context_ =
      std::make_unique<agrpc::GrpcContext>(builder_.AddCompletionQueue());
  control_grpc_context_->work_started();
  data_grpc_context_->work_started();

  builder_.AddListeningPort(host_, grpc::InsecureServerCredentials());
  builder_.RegisterService(&wc_service_);

  server_ = builder_.BuildAndStart();

  if (!server_) {
    throw std::runtime_error("Failed to start gRPC server on " + host_);
  }

  server_shutdown_ = std::make_unique<helper::ServerShutdown>(
      *server_, *control_grpc_context_, &io_context_);
}

void CoordinateServer::init() {
  register_handlers();
  boost::asio::co_spawn(
      timer_.get_executor(),
      [self = shared_from_this()]() -> boost::asio::awaitable<void> {
        co_await self->scheduleRecurringTasks();
      },
      boost::asio::detached);
}

CoordinateServer::~CoordinateServer() {
  if (control_grpc_context_) {
    control_grpc_context_->stop();
  }
  if (data_grpc_context_) {
    data_grpc_context_->stop();
  }
}

auto CoordinateServer::scheduleRecurringTasks()
    -> boost::asio::awaitable<void> {
  while (true) {
    timer_.expires_after(std::chrono::seconds(1));
    boost::system::error_code ec;
    co_await timer_.async_wait(
        boost::asio::redirect_error(boost::asio::use_awaitable, ec));
    if (ec) {
      co_return;
    }
    if (coordinate_state_) {
      coordinate_state_->CheckForInactiveWorker();
    }
  }
}
void CoordinateServer::register_handlers() {

  agrpc::register_awaitable_rpc_handler<PollTaskRPC>(
      *control_grpc_context_, wc_service_,
      [self = shared_from_this()](PollTaskRPC &rpc, wc::PollRequest &request)
          -> boost::asio::awaitable<void> {
        co_await self->handle_poll_task(rpc, request);
        co_return;
      },
      helper::RethrowFirstArg{});

  agrpc::register_awaitable_rpc_handler<HeartBeatRPC>(
      *control_grpc_context_, wc_service_,
      [self = shared_from_this()](
          HeartBeatRPC &rpc,
          wc::HeartBeatRequest &request) -> boost::asio::awaitable<void> {
        co_await self->handle_heart_beat(rpc, request);
        co_return;
      },
      helper::RethrowFirstArg{});

  agrpc::register_awaitable_rpc_handler<CompleteTaskRPC>(
      *control_grpc_context_, wc_service_,
      [self = shared_from_this()](
          CompleteTaskRPC &rpc,
          wc::CompleteRequest &request) -> boost::asio::awaitable<void> {
        co_await self->handle_complete_task(rpc, request);
        co_return;
      },
      helper::RethrowFirstArg{});

  agrpc::register_awaitable_rpc_handler<ReportShuffleErrorRPC>(
      *control_grpc_context_, wc_service_,
      [self = shared_from_this()](
          ReportShuffleErrorRPC &rpc,
          wc::ShuffleErrorReport &request) -> boost::asio::awaitable<void> {
        co_await self->handle_report_shuffle_error(rpc, request);
        co_return;
      },
      helper::RethrowFirstArg{});

  agrpc::register_awaitable_rpc_handler<FetchInputSplitRPC>(
      *data_grpc_context_, wc_service_,
      [self = shared_from_this()](
          FetchInputSplitRPC &rpc,
          wc::SplitDataRequest &request) -> boost::asio::awaitable<void> {
        co_await self->handle_fetch_input_split(rpc, request);
        co_return;
      },
      helper::RethrowFirstArg{});
}

auto CoordinateServer::handle_poll_task(PollTaskRPC &rpc,
                                        wc::PollRequest &request)
    -> boost::asio::awaitable<void> {
  wc::TaskAssignment response =
      coordinate_state_->DistributeTaskToPollingWorker(request.worker_id());
  std::cout << "[Coordinator] PollTask from " << request.worker_id()
            << " -> assigned task_id=" << response.task_id()
            << " type=" << static_cast<int>(response.type()) << std::endl;
  co_await rpc.finish(response, grpc::Status::OK);
}

auto CoordinateServer::handle_heart_beat(HeartBeatRPC &rpc,
                                         wc::HeartBeatRequest &request)
    -> boost::asio::awaitable<void> {
  wc::HeartBeatReply response = coordinate_state_->CheckLastHeartBeat(request);
  co_await rpc.finish(response, grpc::Status::OK);
}

auto CoordinateServer::handle_complete_task(CompleteTaskRPC &rpc,
                                            wc::CompleteRequest &request)
    -> boost::asio::awaitable<void> {
  wc::CompleteReply response =
      coordinate_state_->WorkerReportCompletedMapTask(request);
  std::cout << "[Coordinator] CompleteTask for task_id=" << request.task_id()
            << " accepted=" << response.accepted() << std::endl;
  co_await rpc.finish(response, grpc::Status::OK);
}

auto CoordinateServer::handle_report_shuffle_error(
    ReportShuffleErrorRPC &rpc, wc::ShuffleErrorReport &request)
    -> boost::asio::awaitable<void> {
  wc::ShuffleErrorReply response =
      coordinate_state_->ReducePhaseShuffleError(request);
  co_await rpc.finish(response, grpc::Status::OK);
}

auto CoordinateServer::handle_fetch_input_split(FetchInputSplitRPC &rpc,
                                                wc::SplitDataRequest &request)
    -> boost::asio::awaitable<void> {
  std::cout << "[Coordinator] handle_fetch_input_split called for task_id="
            << request.task_id() << " gen=" << request.generation_id()
            << std::endl;

  if (!coordinate_state_->ValidateSplitRequest(request)) {
    std::cout << "[Coordinator] ValidateSplitRequest FAILED for task_id="
              << request.task_id() << std::endl;
    co_await rpc.finish(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                     "Invalid split request or expired lease"));
    co_return;
  }

  co_await boost::asio::post(thread_pool_, boost::asio::use_awaitable);

  std::cout << "[Coordinator] File opening: " << request.input_file()
            << std::endl;
  std::ifstream file(request.input_file(), std::ios::binary);

  if (!file.is_open()) {
    std::cout << "[Coordinator] File NOT found: " << request.input_file()
              << std::endl;
    co_await rpc.finish(
        grpc::Status(grpc::StatusCode::NOT_FOUND, "Input file not found"));
    co_return;
  }

  file.seekg(request.offset());

  std::uint64_t bytes_remaining = request.length();
  std::cout << "[Coordinator] bytes_remaining to send: " << bytes_remaining
            << std::endl;
  std::vector<char> buffer(64 * 1024); // 64 kb slice
  bool ok = true;

  while (ok && bytes_remaining > 0) {

    co_await boost::asio::post(thread_pool_, boost::asio::use_awaitable);

    std::size_t to_read =
        std::min<std::uint64_t>(bytes_remaining, buffer.size());

    file.read(buffer.data(), to_read);

    std::streamsize bytes_read = file.gcount();
    if (bytes_read <= 0) {
      ok = false;
      break;
    }

    wc::SplitDataChunk chunk;
    chunk.set_payload(buffer.data(), bytes_read);
    //  send slice over TCP with backpressure
    // The tag completes in data_grpc_context_ and resumes there
    bytes_remaining -= bytes_read;
    std::cout << "[Coordinator] Sending chunk of size " << bytes_read << "..."
              << std::endl;
    ok = co_await rpc.write(chunk);
    std::cout << "[Coordinator] rpc.write returned " << ok << std::endl;
  }

  if (ok) {
    std::cout << "[Coordinator] All chunks sent successfully. Finishing RPC OK."
              << std::endl;
    co_await rpc.finish(grpc::Status::OK);
  } else {
    std::cout << "[Coordinator] Issue sending data. Finishing RPC error."
              << std::endl;
    co_await rpc.finish(
        grpc::Status(grpc::StatusCode::NOT_FOUND, "Issue with sending data"));
  }
}

void CoordinateServer::run() {
  if (data_grpc_context_) {
    boost::asio::post(thread_pool_, [this] { data_grpc_context_->run(); });
  }

  agrpc::run(*control_grpc_context_, io_context_);
}
void CoordinateServer::stop() {
  timer_.cancel();

  if (server_shutdown_) {
    server_shutdown_->shutdown();
  }

  io_context_.stop();
}

} // namespace mr