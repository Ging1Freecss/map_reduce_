#include "server.hpp"
#include "mr_coordination.grpc.pb.h"
#include "mr_coordination.pb.h"
#include "shuffle.grpc.pb.h"
#include "state.hpp"
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
#include <boost/asio/error.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/ip/host_name.hpp>
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
WorkerServer::WorkerServer(std::string coordinator_endpoint,
                           std::string listen_address,
                           std::string advertised_host, std::size_t num_threads)
    : coordinator_endpoint_{std::move(coordinator_endpoint)},
      listen_address_{std::move(listen_address)},
      advertised_host_(advertised_host),
      wc_control_stub_(wc::MapReduceService::NewStub(grpc::CreateChannel(
          coordinator_endpoint_, grpc::InsecureChannelCredentials()))),
      wc_data_stub_(wc::MapReduceService::NewStub(grpc::CreateChannel(
          coordinator_endpoint_, grpc::InsecureChannelCredentials()))),
      thread_pool_(num_threads),
      main_work_guard_(boost::asio::make_work_guard(io_context_)),
      bg_work_guard_(boost::asio::make_work_guard(bg_io_context_)) {

  // worker initialisation
  worker_state_.worker_id_ = "worker-" + boost::asio::ip::host_name() + "-" +
                             std::to_string(::getpid());
  worker_state_.advertised_host_ = advertised_host_;

  grpc::ServerBuilder builder;

  control_grpc_context_ =
      std::make_unique<agrpc::GrpcContext>(builder.AddCompletionQueue());
  data_grpc_context_ =
      std::make_unique<agrpc::GrpcContext>(builder.AddCompletionQueue());
  data_grpc_context_->work_started();
  control_grpc_context_->work_started();

  builder.AddListeningPort(listen_address_, grpc::InsecureServerCredentials());
  builder.RegisterService(&data_shuffle_service_);

  server_ = builder.BuildAndStart();

  if (!server_) {
    throw std::runtime_error("Failed to start gRPC server on " +
                             listen_address_);
  }

  server_shutdown_ = std::make_unique<helper::ServerShutdown>(
      *server_, *control_grpc_context_, &io_context_);
}

WorkerServer::~WorkerServer() {
  if (control_grpc_context_) {
    control_grpc_context_->stop();
  }
  if (data_grpc_context_) {
    data_grpc_context_->stop();
  }
}

auto WorkerServer::poll_task_async(wc::PollRequest &request)
    -> boost::asio::awaitable<std::expected<wc::TaskAssignment, grpc::Status>> {

  using RPC =
      agrpc::ClientRPC<&wc::MapReduceService::Stub::PrepareAsyncPollTask>;

  grpc::ClientContext client_context;
  client_context.set_deadline(std::chrono::system_clock::now() +
                              std::chrono::seconds(3));

  wc::TaskAssignment response;
  grpc::Status status =
      co_await RPC::request(*control_grpc_context_, *wc_control_stub_,
                            client_context, request, response);

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

  grpc::Status status =
      co_await RPC::request(*control_grpc_context_, *wc_control_stub_,
                            client_context, request, response);

  if (status.ok()) {
    co_return response;
  }
  co_return std::unexpected(status);
}

auto WorkerServer::poll_complete_task(wc::CompleteRequest &request)
    -> boost::asio::awaitable<std::expected<wc::CompleteReply, grpc::Status>> {

  using RPC =
      agrpc::ClientRPC<&wc::MapReduceService::Stub::PrepareAsyncCompleteTask>;

  grpc::ClientContext client_context;
  client_context.set_deadline(std::chrono::system_clock::now() +
                              std::chrono::seconds(2));

  wc::CompleteReply response;

  grpc::Status status =
      co_await RPC::request(*control_grpc_context_, *wc_control_stub_,
                            client_context, request, response);
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

  std::cout << "[Worker] Starting rpc.start..." << std::endl;
  if (!(co_await rpc.start(*wc_data_stub_, request))) {
    std::cout << "[Worker] rpc.start FAILED!" << std::endl;
    co_return co_await rpc.finish();
  }
  std::cout << "[Worker] rpc.start SUCCEEDED!" << std::endl;

  auto channel =
      std::make_shared<ChunkChannel>(co_await boost::asio::this_coro::executor,
                                     64); // 64 * 64kb = 4mb

  // create data file
  const std::string map_data_filename = worker_state_.GetMapTaskInputFileName(
      request.input_file(), request.task_id());

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

  std::cout << "[Worker] Spawning disk writer coroutine..." << std::endl;
  boost::asio::co_spawn(
      disk_strand,
      [channel, done_ch, map_data_filename,
       disk_cancel_signal]() -> boost::asio::awaitable<void> {
        std::cout << "[Worker] Disk writer coroutine STARTED for "
                  << map_data_filename << std::endl;
        std::ofstream file;

        auto return_value = [&](bool val) -> boost::asio::awaitable<void> {
          // very important, if signal.emit is called , then any co_await
          // after that will abort but we need to send if this coroutine
          // fails so we disable cancellation only when we are sending
          // final output, inside the while loop its fine to abourt as
          // we catch it
          if (file.is_open()) {
            file.close();
          }

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
            channel->close();
            co_await return_value(false);
            co_return;
          }
        }

        file.open(map_data_filename, std::ios::binary | std::ios::trunc);
        if (!file.is_open()) {
          channel->close();
          co_await return_value(false);
          co_return;
        }

        while (true) {
          auto [ec, payload] = co_await channel->async_receive(
              boost::asio::as_tuple(boost::asio::use_awaitable));

          if (ec) {

            // check if the channel was closed due to cancellation:
            auto cancel_state =
                co_await boost::asio::this_coro::cancellation_state;
            if (cancel_state.cancelled() !=
                boost::asio::cancellation_type::none) {

              channel->close();
              co_await return_value(false);
              co_return;
            }

            if (ec == boost::asio::experimental::error::channel_closed) {
              // normal exit, parent close the channel after sending
              // all chunks
              break;
            }
            channel->close();
            co_await return_value(false);
            co_return;
          }

          file.write(payload.data(), payload.size());
          if (file.fail()) {
            channel->close();
            co_await return_value(false);
            co_return;
          }
        }

        co_await return_value(!file.fail());
        co_return;
      },
      // bind the disk coroutine to disk_cancel_sig
      boost::asio::bind_cancellation_slot(disk_cancel_signal->slot(),
                                          boost::asio::detached));

  // receive from network
  wc::SplitDataChunk chunk;
  bool stream_aborted{false};

  std::cout << "[Worker] Calling co_await rpc.read(chunk)..." << std::endl;
  while (co_await rpc.read(chunk)) {
    std::cout << "[Worker] Read chunk with " << chunk.payload().size()
              << " bytes" << std::endl;
    auto [send_ec] = co_await channel->async_send(
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

    channel->cancel();
    rpc.cancel();
  }

  channel->close();

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

  auto ex = co_await boost::asio::this_coro::executor;

  while (true) {
    wc::PollRequest request;
    request.set_worker_id(worker_state_.worker_id_);

    std::expected<wc::TaskAssignment, grpc::Status> res =
        co_await poll_task_async(request);

    if (!res)
      throw std::runtime_error("issue in polling");

    // set task in worker state
    wc::TaskAssignment &task = res.value();

    if (task.type() == wc::TaskType::WAIT) {
      boost::asio::steady_timer delay{
          co_await boost::asio::this_coro::executor};

      delay.expires_after(poll_task_wait_interval);

      co_await delay.async_wait(boost::asio::use_awaitable);
      continue;
    } else if (task.type() == wc::TaskType::MAP) {
      std::cout << "[Worker] Assigned MAP task " << task.task_id() << " (gen "
                << task.generation_id() << ", offset " << task.offset()
                << ", len " << task.length() << ")" << std::endl;

      wc::SplitDataRequest split_data_request;

      split_data_request.set_task_id(task.task_id());
      split_data_request.set_generation_id(task.generation_id());
      split_data_request.set_input_file(task.input_file());
      split_data_request.set_offset(task.offset());
      split_data_request.set_length(task.length());

      worker_state_.SetMapState(task);

      // download data
      std::cout << "[Worker] Fetching map data from coordinator..."
                << std::endl;
      grpc::Status download_status = co_await boost::asio::co_spawn(
          bg_io_context_, fetch_map_data(split_data_request),
          boost::asio::use_awaitable);

      if (!download_status.ok()) {
        std::cerr << "[Worker] Fetch map data failed: "
                  << download_status.error_message() << std::endl;
        throw std::runtime_error("error in downloading map data");
      }

      std::cout
          << "[Worker] Map data downloaded successfully. Executing mapper..."
          << std::endl;
      // in thread pool , execute map task
      co_await boost::asio::post(thread_pool_, boost::asio::use_awaitable);
      std::optional<wc::CompleteRequest> map_res =
          worker_state_.ExecuteMapTask();

      if (!map_res)
        throw std::runtime_error("error in map phase");

      // Switch back to main executor to call gRPC
      co_await boost::asio::post(ex, boost::asio::use_awaitable);

      wc::CompleteRequest &complete_request = map_res.value();
      std::cout << "[Worker] Reporting completion with "
                << complete_request.output_paths_size()
                << " output partition(s)..." << std::endl;

      auto res_complete_task = co_await poll_complete_task(complete_request);

      if (!res_complete_task) {
        throw std::runtime_error("error in poll_complete_task, grpc::Status: " +
                                 res_complete_task.error().error_message());
      }

      wc::CompleteReply &complete_reply = res_complete_task.value();
      if (!complete_reply.accepted()) {
        throw std::runtime_error("Coordinator rejected CompleteTask");
      }

      std::cout << "[Worker] Map task " << task.task_id()
                << " completed and accepted by coordinator!" << std::endl;
      std::error_code rm_ec;
      std::filesystem::remove(worker_state_.GetMapTaskInputFileName(
                                  task.input_file(), task.task_id()),
                              rm_ec);
      worker_state_.ResetWorkerState();
    } else if (task.type() == wc::TaskType::REDUCE) {
      std::cout << "[Worker] Assigned REDUCE task " << task.task_id()
                << " with " << task.intermediate_paths_size()
                << " partitions. (Reduce phase pending implementation)"
                << std::endl;
      boost::asio::steady_timer delay{
          co_await boost::asio::this_coro::executor};
      delay.expires_after(std::chrono::seconds(5));
      co_await delay.async_wait(boost::asio::use_awaitable);
      continue;
    }
  }
  co_return;
}

auto WorkerServer::start_heart_beats() -> boost::asio::awaitable<void> {

  while (true) {

    wc::HeartBeatRequest request;
    request.set_worker_id(worker_state_.worker_id_);

    if (worker_state_.task_assignment_) {
      request.set_task_type(worker_state_.task_assignment_->type());
      request.set_task_id(worker_state_.task_assignment_->task_id());
      request.set_generation_id(
          worker_state_.task_assignment_->generation_id());
    } else {
      request.set_task_type(wc::TaskType::WAIT);
    }

    auto res = co_await poll_heart_beat(request);

    if (res && res.value().acknowledged()) {

      boost::asio::steady_timer timer(
          co_await boost::asio::this_coro::executor);

      timer.expires_after(heart_beat_interval);

      co_await timer.async_wait(boost::asio::use_awaitable);

    } else {
      // coordinator rejected task or network issue

      if (signal_) {
        signal_->emit(boost::asio::cancellation_type::terminal);
      }

      boost::asio::steady_timer retry_timer{
          co_await boost::asio::this_coro::executor, heart_beat_interval};
      co_await retry_timer.async_wait(boost::asio::use_awaitable);
    }
  }
}

auto WorkerServer::run_worker_with_heartbeats()
    -> boost::asio::awaitable<void> {

  auto ex = co_await boost::asio::this_coro::executor;

  while (true) {

    // allocate a fresh signal for each session
    // once signal has emitted ,it is spent and cant be reused

    signal_ = std::make_shared<boost::asio::cancellation_signal>();
    bool was_aborted = false;

    try {

      co_await boost::asio::co_spawn(
          ex, start_worker(),
          boost::asio::bind_cancellation_slot(signal_->slot(),
                                              boost::asio::use_awaitable));
      break;
    } catch (const boost::system::system_error &e) {

      // worker session was cancelled by heartbeat
      // reset state so the worker is clean for next attempt
      if (e.code() == boost::asio::error::operation_aborted) {
        was_aborted = true;
      } else {
        throw;
      }
    }

    if (was_aborted) {

      // reset worker state
      worker_state_.ResetWorkerState();

      // Backoff delay before restarting start_worker():
      boost::asio::steady_timer backoff{ex, std::chrono::milliseconds(500)};
      co_await backoff.async_wait(boost::asio::use_awaitable);
    }
  }
}

void WorkerServer::run() {

  boost::asio::co_spawn(
      io_context_,
      [&]() -> boost::asio::awaitable<void> {
        co_await run_worker_with_heartbeats();
      },
      boost::asio::detached);

  boost::asio::co_spawn(
      io_context_,
      [&]() -> boost::asio::awaitable<void> { co_await start_heart_beats(); },
      boost::asio::detached);

  background_thread_ = std::jthread([self = shared_from_this()]() {
    std::cout << "[Worker] background_thread_ started" << std::endl;
    agrpc::run(*self->data_grpc_context_, self->bg_io_context_);
    std::cout << "[Worker] background_thread_ EXITED!" << std::endl;
  });

  agrpc::run(*control_grpc_context_, io_context_);
}

} // namespace mr