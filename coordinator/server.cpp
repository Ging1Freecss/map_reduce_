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
#include <boost/asio/experimental/channel_error.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
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

  co_await boost::asio::this_coro::throw_if_cancelled(false);

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

  std::cout << "[Coordinator] File opening: " << request.input_file()
            << std::endl;

  using ChunkChannel = boost::asio::experimental::concurrent_channel<void(
      boost::system::error_code, wc::SplitDataChunk)>;

  using DoneChannel = boost::asio::experimental::concurrent_channel<void(
      boost::system::error_code, grpc::Status)>;

  auto io_executor = co_await boost::asio::this_coro::executor;
  auto channel =
      std::make_shared<ChunkChannel>(io_executor, 64); // 64*64kb = 4mb buffer
  auto done_ch = std::make_shared<DoneChannel>(io_executor, 1);

  auto disk_strand = boost::asio::make_strand(thread_pool_);
  // producer: to read from disk
  boost::asio::co_spawn(
      disk_strand,
      [channel, done_ch, request,
       self = shared_from_this()]() -> boost::asio::awaitable<void> {
        co_await boost::asio::this_coro::throw_if_cancelled(false);

        std::ifstream file;
        auto return_value = [&](grpc::Status val) {
          if (file.is_open()) {
            file.close();
          }
          channel->close();
          done_ch->try_send(boost::system::error_code{}, val);
          done_ch->close();
        };

        file.open(request.input_file(), std::ios::binary);
        if (!file.is_open()) {
          return_value(grpc::Status(grpc::StatusCode::NOT_FOUND,
                                    "Input file not found"));
          co_return;
        }

        file.seekg(request.offset());
        if (file.fail()) {
          return_value(grpc::Status(grpc::StatusCode::INTERNAL,
                                    "Failed to seek input file offset"));
          co_return;
        }

        std::uint64_t bytes_remaining = request.length();
        std::vector<char> buffer(64 * 1024);
        grpc::Status final_status = grpc::Status::OK;

        while (bytes_remaining > 0) {
          std::size_t to_read =
              std::min<std::uint64_t>(bytes_remaining, buffer.size());

          file.read(buffer.data(), static_cast<std::streamsize>(to_read));

          std::streamsize bytes_read = file.gcount();

          if (bytes_read <= 0) {
            return_value(grpc::Status(grpc::StatusCode::DATA_LOSS,
                                      "Unexpected EOF while reading split"));
            co_return;
          }

          bytes_remaining -= bytes_read;
          wc::SplitDataChunk chunk;
          chunk.set_payload(buffer.data(), bytes_read);

          auto [send_ec] = co_await channel->async_send(
              boost::system::error_code{}, std::move(chunk),
              boost::asio::as_tuple(boost::asio::use_awaitable));

          if (send_ec) {
            return_value(grpc::Status(grpc::StatusCode::ABORTED,
                                      "Streaming aborted on channel send"));
            co_return;
          }
        }

        return_value(grpc::Status::OK);
      },
      [channel, done_ch](std::exception_ptr ep) {
        channel->close();
        if (ep) {
          done_ch->try_send(boost::system::error_code{},
                            grpc::Status(grpc::StatusCode::INTERNAL,
                                         "Exception in split reader task"));
          done_ch->close();
        }
      });

  // consumer , send data to client
  bool stream_aborted = false;

  while (true) {
    auto [recv_ec, chunk] = co_await channel->async_receive(
        boost::asio::as_tuple(boost::asio::use_awaitable));

    if (recv_ec) {

      // EOF from producer
      if (recv_ec == boost::asio::experimental::error::channel_closed) {
        break;
      }

      stream_aborted = true;
      break;
    }

    bool ok = co_await rpc.write(chunk);

    if (!ok) {
      stream_aborted = true;
      channel->cancel();
      channel->close();
      break;
    }
  }

  if (stream_aborted) {

    channel->cancel();
    channel->close();
  }

  auto [done_ec, disk_status] = co_await done_ch->async_receive(
      boost::asio::as_tuple(boost::asio::use_awaitable));

  if (stream_aborted) {

    if (rpc.context().IsCancelled()) {
      co_await rpc.finish(grpc::Status(grpc::StatusCode::CANCELLED,
                                       "Split streaming was cancelled"));
    } else {
      co_await rpc.finish(grpc::Status(grpc::StatusCode::ABORTED,
                                       "Network stream interrupted"));
    }
  } else if (done_ec) {
    co_await rpc.finish(grpc::Status(grpc::StatusCode::INTERNAL,
                                     "Disk reader channel closed prematurely"));
  } else if (!disk_status.ok()) {
    co_await rpc.finish(disk_status);
  } else {
    co_await rpc.finish(grpc::Status::OK);
  }
}

void CoordinateServer::run() {
  if (data_grpc_context_) {
    data_thread_ = std::jthread{
        [self = shared_from_this()] { self->data_grpc_context_->run(); }};
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