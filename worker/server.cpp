#include "server.hpp"
#include "mr_coordination.grpc.pb.h"
#include "mr_coordination.pb.h"
#include "shuffle.grpc.pb.h"
#include <agrpc/client_rpc.hpp>
#include <agrpc/detail/forward.hpp>
#include <agrpc/grpc_context.hpp>
#include <agrpc/run.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_state.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/detail/error_code.hpp>
#include <chrono>
#include <cstddef>
#include <fstream>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/support/status.h>
#include <memory>
#include <stdexcept>
#include <system_error>
#include <thread>

/*
     mr_stub_(mr_proto::MapReduceService::NewStub(
          grpc::CreateChannel(host, grpc::InsecureChannelCredentials()))) {}
*/
namespace mr {
WorkerServer::WorkerServer(std::string host, std::size_t num_threads)
    : host_{std::move(host)},
      wc_stub_(wc::MapReduceService::NewStub(
          grpc::CreateChannel(host_, grpc::InsecureChannelCredentials()))),
      ww_stub_(ww::WorkerShuffleService::NewStub(
          grpc::CreateChannel(host_, grpc::InsecureChannelCredentials()))),
      thread_pool_(num_threads),
      main_work_guard_(boost::asio::make_work_guard(io_context_)),
      bg_work_guard_(boost::asio::make_work_guard(bg_io_context_)),
      signal_{std::make_shared<boost::asio::cancellation_signal>()} {

  grpc::ServerBuilder builder;

  control_grpc_context_ =
      std::make_unique<agrpc::GrpcContext>(builder.AddCompletionQueue());

  data_grpc_context_ =
      std::make_unique<agrpc::GrpcContext>(builder.AddCompletionQueue());
  builder.AddListeningPort(host, grpc::InsecureServerCredentials());
  builder.RegisterService(&data_service_);
  server_ = builder.BuildAndStart();

  if (!server_) {
    throw std::runtime_error("Failed to start gRPC server on " + host_);
  }

  server_shutdown_ = std::make_unique<helper::ServerShutdown>(
      *server_, *control_grpc_context_, &io_context_);
}

auto WorkerServer::poll_task_async(wc::PollRequest &request)
    -> boost::asio::awaitable<std::expected<wc::TaskAssignment, grpc::Status>> {

  using RPC =
      agrpc::ClientRPC<&wc::MapReduceService::Stub::PrepareAsyncPollTask>;

  grpc::ClientContext client_context;
  client_context.set_deadline(std::chrono::system_clock::now() +
                              std::chrono::seconds(3));

  wc::TaskAssignment response;
  grpc::Status status = co_await RPC::request(
      *control_grpc_context_, *wc_stub_, client_context, request, response);

  if (status.ok()) {
    co_return response;
  }
  co_return std::unexpected(status);
}

auto WorkerServer::poll_heart_beat(wc::HeartBeatRequest &request)
    -> boost::asio::awaitable<std::expected<wc::HeartBeatReply, grpc::Status>> {

  using RPC =
      agrpc::ClientRPC<&wc::MapReduceService::Stub::PrepareAsyncHeartbeat>;

  grpc::ClientContext client_context;
  client_context.set_deadline(std::chrono::system_clock::now() +
                              std::chrono::seconds(2));
  wc::HeartBeatReply response;

  if (!worker_state_.task_assignment_.has_value()) {
    response.set_abort_task(true);
    response.set_acknowledged(false);
    co_return response;
  }
  grpc::Status status = co_await RPC::request(
      *control_grpc_context_, *wc_stub_, client_context, request, response);

  if (status.ok()) {
    co_return response;
  }
  co_return std::unexpected(status);
}

/*
  download data from coordinator
  poll the task in thread in which data_grpc_context_ polls in
  and uses a thread pool to write to the temp file

  VERY IMPORTANT:
  this function should be called with cancellation slot assign to
  its coroutine like make_parallel_group or boost::asio::post
  with bind cancellation etc NOT RAW and caller needs to handle
  boost::system::system_error
*/
auto WorkerServer::fetch_map_data(wc::SplitDataRequest &request)
    -> boost::asio::awaitable<grpc::Status> {
  using RPC = agrpc::ClientRPC<
      &wc::MapReduceService::Stub::PrepareAsyncFetchInputSplit>;

  using ChunkChannel = boost::asio::experimental::concurrent_channel<void(
      boost::system::error_code, std::string)>;

  // coroutine and its child type unchange , just let pass as it is
  co_await boost::asio::this_coro::reset_cancellation_state(
      boost::asio::enable_total_cancellation());
  RPC rpc{*data_grpc_context_};

  if (!(co_await rpc.start(*wc_stub_, request))) {
    co_return co_await rpc.finish();
  }

  ChunkChannel channel{co_await boost::asio::this_coro::executor,
                       64}; // 64 * 64kb = 4mb

  // create data file
  const std::string map_data_filename =
      request.input_file() + ".map_data." + std::to_string(request.task_id());

  // cancellation using signal
  /*
    since we are using thread pool we need to create
    a strand for so disk coroutine and cancellation ,so it can
    run one at a time
  */

  auto disk_strand = boost::asio::make_strand(thread_pool_);

  // signal for disk write in thread pool
  auto disk_cancel_signal =
      std::make_shared<boost::asio::cancellation_signal>();

  // to get the output of writing to disk function below
  using DoneChannel = boost::asio::experimental::concurrent_channel<void(
      boost::system::error_code, bool)>;
  auto done_ch = std::make_shared<DoneChannel>(
      co_await boost::asio::this_coro::executor, 1);

  boost::asio::co_spawn(
      disk_strand,
      [&channel, done_ch, map_data_filename]() -> boost::asio::awaitable<void> {
        auto return_value = [&](bool val) -> boost::asio::awaitable<void> {
          // very important, if signal.emit is called , then any co_await
          // after that will abort but we need to send if this coroutine
          // fails so we disable cancellation only when we are sending
          // final output, inside the while loop its fine to abourt as
          // we catch it
          co_await boost::asio::this_coro::reset_cancellation_state(
              boost::asio::disable_cancellation());
          co_await done_ch->async_send(
              boost::system::error_code{}, val,
              boost::asio::as_tuple(boost::asio::use_awaitable));
        };
        // create parent directory if not present

        std::error_code fs_ec;
        std::filesystem::path p{map_data_filename};
        if (p.has_parent_path()) {
          std::filesystem::create_directories(p.parent_path(), fs_ec);
          if (fs_ec) {
            channel.close();
            co_await return_value(false);
            co_return;
          }
        }

        std::ofstream file{map_data_filename,
                           std::ios::binary | std::ios::trunc};

        if (!file.is_open()) {
          channel.close();
          co_await return_value(false);
          co_return;
        }

        while (true) {
          auto [ec, payload] = co_await channel.async_receive(
              boost::asio::as_tuple(boost::asio::use_awaitable));

          if (ec) {

            // check if the channel was closed due to cancellation:
            auto cancel_state =
                co_await boost::asio::this_coro::cancellation_state;
            if (cancel_state.cancelled() !=
                boost::asio::cancellation_type::none) {

              channel.close();
              co_await return_value(false);
              co_return;
            }

            if (ec == boost::asio::experimental::error::channel_closed) {
              // normal exit, parent close the channel after sending
              // all chunks
              break;
            }
            channel.close();
            co_await return_value(false);
            co_return;
          }

          file.write(payload.data(), payload.size());
          if (file.fail()) {
            channel.close();
            co_await return_value(false);
            co_return;
          }
        }

        file.close();

        co_await return_value(!file.fail());
        co_return;
      },
      // bind the disk coroutine to disk_cancel_sig
      boost::asio::bind_cancellation_slot(disk_cancel_signal->slot(),
                                          boost::asio::detached));

  // receive from network
  wc::SplitDataChunk chunk;
  bool stream_aborted{false};

  while (co_await rpc.read(chunk)) {
    auto [send_ec] = co_await channel.async_send(
        boost::system::error_code{}, std::move(*chunk.mutable_payload()),
        boost::asio::as_tuple(boost::asio::use_awaitable));

    if (send_ec) {
      // Disk coroutine failed or cancellation is called
      // and closed the channel
      stream_aborted = true;
      break;
    }
  }

  // refers to heartbeat calling cancellation
  auto cancel_state = co_await boost::asio::this_coro::cancellation_state;

  // cancel state is a pointer, make sure to copy here or else lead to error
  // when we called   co_await boost::asio::this_coro::reset_cancellation_state(
  //    boost::asio::disable_cancellation());
  const bool was_cancelled =
      (cancel_state.cancelled() != boost::asio::cancellation_type::none);

  // very important as if heartbeat has cancel in this coroutine
  // then any co_await on fetch_map will abort immediately
  // need to remove them to properly check final output
  co_await boost::asio::this_coro::reset_cancellation_state(
      boost::asio::disable_cancellation());

  if (was_cancelled     // signal by heartbeat
      || stream_aborted // issue with writing data to disk
  ) {

    // important that write to disk cancellation happens with strand
    // as race condition may arise, also make sure to co_await
    // so channel.cancel() run after this , since it will
    // wakes up disk writer so then the disk writer will return false
    co_await boost::asio::co_spawn(
        disk_strand,
        [disk_cancel_signal]() -> boost::asio::awaitable<void> {
          disk_cancel_signal->emit(boost::asio::cancellation_type::terminal);
          co_return;
        },
        boost::asio::use_awaitable);

    channel.cancel();
    rpc.cancel();
  }

  channel.close();

  grpc::Status network_status = co_await rpc.finish();

  auto [ec, disk_completed] = co_await done_ch->async_receive(
      boost::asio::as_tuple(boost::asio::use_awaitable));

  done_ch->close();

  /*
    The root-cause order must be checked from external intent → local disk →
    remote network:
  */
  if (stream_aborted || !disk_completed || !network_status.ok()) {

    // go to disk_strand thread_pool_ for file related work
    co_await boost::asio::post(disk_strand, boost::asio::use_awaitable);

    std::error_code rm_ec;
    std::filesystem::remove(map_data_filename, rm_ec);

    // cancelled from outside
    if (was_cancelled) {
      co_return grpc::Status(grpc::StatusCode::CANCELLED,
                             "Fetch map data cancelled by heartbeat or parent");
    }

    if (!disk_completed) {
      co_return grpc::Status(
          grpc::StatusCode::DATA_LOSS,
          "Failed while writing split to disk (disk full or write error)");
    }

    if (!network_status.ok()) {
      co_return network_status;
    }

    co_return grpc::Status(grpc::StatusCode::ABORTED, "issue in fetch_map");
  }
  co_return network_status;
}

auto WorkerServer::start_worker() -> boost::asio::awaitable<void> {

  while (true) {
    wc::PollRequest request;
    co_await poll_task_async(request);
  }
  co_return;
}

auto WorkerServer::start_heart_beats() -> boost::asio::awaitable<void> {

  while (true) {

    wc::HeartBeatRequest request;
    request.set_allocated_worker_id(&worker_state_.worker_id_);
    request.set_task_type(wc::TaskType::WAIT);

    auto res = co_await poll_heart_beat(request);

    if (res) {
      if (res.value().acknowledged()) {
        boost::asio::steady_timer timer(
            co_await boost::asio::this_coro::executor);

        timer.expires_after(
            std::chrono::seconds(heart_beat_intervals_in_seconds));

        co_await timer.async_wait(boost::asio::use_awaitable);
      } else {
      }
    } else {
      throw std::runtime_error("error in heart beat" +
                               res.error().error_message());
    }
  }
}

void WorkerServer::run() {
  boost::asio::co_spawn(
      io_context_,
      [&]() -> boost::asio::awaitable<void> { co_await start_worker(); },
      boost::asio::detached);

  background_thread_ = std::jthread([self = shared_from_this()]() {
    agrpc::run(*self->data_grpc_context_, self->bg_io_context_);
  });

  agrpc::run(*control_grpc_context_, io_context_);
}

} // namespace mr