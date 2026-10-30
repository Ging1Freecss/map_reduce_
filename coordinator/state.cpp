#include "state.hpp"
#include "mr_coordination.pb.h"
#include "storage/error.hpp"
#include "storage/split_reader.hpp"
#include <chrono>
#include <cstddef>
#include <optional>
#include <stdexcept>

namespace mr {

// ============================================================================
// Constructor: Job Initialization & Input Splitting
// ============================================================================
/**
 * @brief Initializes the Coordinator state machine and sets up task catalogs.
 *
 * Execution Steps:
 * 1. Sets initial phase to CONFIGURE and stores job parameters.
 * 2. Invokes generate_text_splits() to partition the master input file into
 *    line-aligned byte ranges (target ~64 MB each) without cutting words.
 * 3. Populates the map_tasks catalog with IDLE tasks mapped 1:1 to input
 * splits.
 * 4. Populates the reduce_tasks catalog with IDLE tasks (R partitions).
 * 5. Transitions cluster phase to MAP to begin accepting worker PollTask RPCs.
 *
 * @param input_path          Path to master raw text input file on Coordinator.
 * @param reduce_partition    Number of reduce partitions (R).
 * @param target_split_bytes  Target split size in bytes (defaults to 64 MB).
 */
CoordinatorState::CoordinatorState(std::filesystem::path input_path,
                                   std::size_t reduce_partition,
                                   std::uint64_t target_split_bytes) {
  job_conf.phase = Phase::CONFIGURE;
  job_conf.input_path = input_path;
  job_conf.num_reduce_partition = reduce_partition;
  job_conf.target_split_bytes = target_split_bytes;
  curr_generation_id = 1;

  std::expected<std::vector<InputSplit>, StorageError> res =
      generate_text_splits(job_conf.input_path, job_conf.target_split_bytes);

  if (!res) {
    throw std::runtime_error("Exception in Coordinator constructor: "
                             "issue generating input splits: " +
                             std::string{storage_erro_to_string(res.error())});
  }

  std::vector<InputSplit> input_splits = std::move(res.value());
  job_conf.num_map_tasks = input_splits.size();

  // Populate initial Map task catalog
  for (std::size_t i = 0; i < input_splits.size(); i++) {
    TaskRecord task_rec{.task_id = static_cast<std::int64_t>(i),
                        .type = wc::TaskType::MAP,
                        .state = State::IDLE};
    map_tasks.push_back(
        MapTask{.task_rec = task_rec, .split = input_splits[i]});
  }

  // Populate initial Reduce task catalog
  for (std::size_t i = 0; i < reduce_partition; i++) {
    TaskRecord task_rec{.task_id = static_cast<std::int64_t>(i),
                        .type = wc::TaskType::REDUCE,
                        .state = State::IDLE};
    reduce_tasks.push_back(ReduceTask{.task_rec = task_rec});
  }

  job_conf.phase = Phase::MAP;
}

// RPC Handler: DistributeTaskToPollingWorker (PollTask)
/**
  @brief Assigns the next available task to a polling idle worker.

   MAP / SHUFFLEFIX Phase:
     Map Tasks
  input_file.
     - If all map tasks are currently leased (IN_PROGRESS), returns WAIT.

  REDUCE Phase:
    return the first IDLE Reduce task.
 * @param worker_id Unique identifier of the polling worker.
 * @return wc::TaskAssignment Protobuf response containing assigned work or
 * directive.
 */
auto CoordinatorState::DistributeTaskToPollingWorker(std::string worker_id)
    -> wc::TaskAssignment {

  if (job_conf.phase == Phase::MAP || job_conf.phase == Phase::SHUFFLEFIX) {
    std::optional<std::size_t> available_task_idx = std::nullopt;
    for (std::size_t i = 0; i < map_tasks.size(); i++) {
      if (map_tasks[i].task_rec.state == State::IDLE) {
        available_task_idx = i;
        break;
      }
    }

    if (!available_task_idx) {
      wc::TaskAssignment task;
      task.set_type(wc::TaskType::WAIT);
      return task;
    }

    auto idx = available_task_idx.value();
    map_tasks[idx].generation_id = curr_generation_id++;
    map_tasks[idx].worker_id = worker_id;
    map_tasks[idx].last_heart_beat = std::chrono::steady_clock::now();
    map_tasks[idx].task_rec.state = State::IN_PROGRESS;

    wc::TaskAssignment task;
    task.set_num_map_tasks(job_conf.num_map_tasks);
    task.set_task_id(static_cast<std::int64_t>(idx));
    task.set_generation_id(map_tasks[idx].generation_id);
    task.set_type(wc::TaskType::MAP);
    task.set_offset(map_tasks[idx].split.offset);
    task.set_length(map_tasks[idx].split.length);
    task.set_num_reduce_tasks(job_conf.num_reduce_partition);
    task.set_input_file(job_conf.input_path.string());

    return task;
  } else if (job_conf.phase == Phase::REDUCE) {
    std::optional<std::size_t> available_task_idx = std::nullopt;
    for (std::size_t i = 0; i < reduce_tasks.size(); i++) {
      if (reduce_tasks[i].task_rec.state == State::IDLE) {
        available_task_idx = i;
        break;
      }
    }

    if (!available_task_idx) {
      wc::TaskAssignment task;
      task.set_type(wc::TaskType::WAIT);
      return task;
    }

    auto idx = available_task_idx.value();
    reduce_tasks[idx].generation_id = curr_generation_id++;
    reduce_tasks[idx].worker_id = worker_id;
    reduce_tasks[idx].last_heart_beat = std::chrono::steady_clock::now();
    reduce_tasks[idx].task_rec.state = State::IN_PROGRESS;

    wc::TaskAssignment task;
    task.set_num_map_tasks(job_conf.num_map_tasks);
    task.set_task_id(static_cast<std::int64_t>(idx));
    task.set_generation_id(reduce_tasks[idx].generation_id);
    task.set_type(wc::TaskType::REDUCE);
    task.set_num_reduce_tasks(job_conf.num_reduce_partition);

    reduce_tasks[idx].all_files_required.clear();
    for (std::size_t i = 0; i < map_tasks.size(); i++) {
      if (idx < map_tasks[i].intermediate_op_file.size()) {
        const auto &uri = map_tasks[i].intermediate_op_file[idx];
        task.add_intermediate_paths(uri);
        reduce_tasks[idx].all_files_required.push_back(uri);
      }
    }
    return task;
  } else if (job_conf.phase == Phase::DONE) {
    wc::TaskAssignment task;
    task.set_type(wc::TaskType::SHUTDOWN);
    return task;
  }

  wc::TaskAssignment task;
  task.set_type(wc::TaskType::WAIT);
  return task;
}

// RPC Handler: WorkerReportCompletedMapTask (CompleteTask)
/**
 * @brief Handles task completion and atomic commit notifications from workers.

  Lease Fencing & Commit Protocol:
  Validates task_id bounds.
  Fencing check: verifies request.generation_id matches active lease. Late
  commits from timed-out zombie workers are rejected immediately.
  Idempotency: prevents duplicate completion increments.

  For Map Tasks:
  - Records advertised P2P intermediate URIs ("<IP>:<Port>/mr-<M>_<R>.data").
  - Sets state to COMPLETED and increments map_tasks_completed.
  - If all Map tasks are complete, advances phase to REDUCE.

  For Reduce Tasks:
  - Records final committed output path ("mr-out-<R>").
  - Sets state to COMPLETED and increments reduce_tasks_completed.
  - If all Reduce tasks are complete, advances phase to DONE.

  @param request CompleteRequest proto containing task_id, generation_id, type,
  output_paths.
  @return wc::CompleteReply Accepted boolean flag.
 */
auto CoordinatorState::WorkerReportCompletedMapTask(
    const wc::CompleteRequest &request) -> wc::CompleteReply {
  wc::CompleteReply reply;

  if (request.type() == wc::TaskType::MAP) {
    if (request.task_id() < 0 ||
        static_cast<std::size_t>(request.task_id()) >= map_tasks.size()) {
      reply.set_accepted(false);
      return reply;
    }

    if (request.output_paths_size() !=
        static_cast<int>(job_conf.num_reduce_partition)) {
      reply.set_accepted(false);
      return reply;
    }

    MapTask &task = map_tasks[request.task_id()];
    if (request.generation_id() != task.generation_id ||
        task.task_rec.state == State::COMPLETED) {
      reply.set_accepted(false);
      return reply;
    }

    task.task_rec.state = State::COMPLETED;
    task.intermediate_op_file.clear();
    int count = request.output_paths_size();

    for (int i = 0; i < count; i++) {
      task.intermediate_op_file.push_back(request.output_paths(i));
    }

    map_tasks_completed++;

    if (CheckIfMapPhaseCompleted()) {
      job_conf.phase = Phase::REDUCE;
    }
    reply.set_accepted(true);
    return reply;

  } else if (request.type() == wc::TaskType::REDUCE) {
    if (request.task_id() < 0 ||
        static_cast<std::size_t>(request.task_id()) >= reduce_tasks.size()) {
      reply.set_accepted(false);
      return reply;
    }

    ReduceTask &task = reduce_tasks[request.task_id()];
    if (request.generation_id() != task.generation_id ||
        task.task_rec.state == State::COMPLETED) {
      reply.set_accepted(false);
      return reply;
    }

    if (request.output_paths_size() != 1) {
      reply.set_accepted(false);
      return reply;
    }

    reduce_tasks_completed++;
    task.task_rec.state = State::COMPLETED;
    task.output_path = request.output_paths(0);

    if (CheckIfReducePhaseCompleted()) {
      job_conf.phase = Phase::DONE;
    }
    reply.set_accepted(true);
    return reply;
  }

  reply.set_accepted(false);
  return reply;
}

// RPC Handler: CheckLastHeartBeat (Heartbeat)
/**
  @brief Validates worker liveness and resets the 10-second lease watchdog.

  @param request HeartBeatRequest proto (worker_id, task_id, generation_id,
  task_type).
  @return wc::HeartBeatReply Acknowledgement and abort flags.
 */
auto CoordinatorState::CheckLastHeartBeat(wc::HeartBeatRequest &request)
    -> wc::HeartBeatReply {
  wc::HeartBeatReply reply;

  if (request.task_type() == wc::TaskType::MAP &&
      (request.task_id() >= 0 && request.task_id() < map_tasks.size())) {

    auto &task = map_tasks[request.task_id()];
    if (request.generation_id() == task.generation_id &&
        request.worker_id() == task.worker_id &&
        task.task_rec.state == State::IN_PROGRESS) {
      reply.set_acknowledged(true);
      reply.set_abort_task(false);
      task.last_heart_beat = std::chrono::steady_clock::now();
      return reply;
    }

  } else if (request.task_type() == wc::TaskType::REDUCE &&
             (request.task_id() >= 0 &&
              request.task_id() < reduce_tasks.size())) {

    auto &task = reduce_tasks[request.task_id()];
    if (request.generation_id() == task.generation_id &&
        request.worker_id() == task.worker_id &&
        task.task_rec.state == State::IN_PROGRESS) {
      reply.set_acknowledged(true);
      reply.set_abort_task(false);
      task.last_heart_beat = std::chrono::steady_clock::now();
      return reply;
    }
  }

  reply.set_acknowledged(false);
  reply.set_abort_task(true);
  return reply;
}

auto CoordinatorState::CheckIfMapPhaseCompleted() -> bool {
  return map_tasks_completed == map_tasks.size();
}

auto CoordinatorState::CheckIfReducePhaseCompleted() -> bool {
  return reduce_tasks_completed == reduce_tasks.size();
}

// RPC Handler: ReducePhaseShuffleError (ReportShuffleError)
/**
 * @brief Handles P2P shuffle download failures reported by Reduce workers.

  Cascade Invalidation Protocol:
  1. Identifies the failed Map task that produced the unreachable partition.
  2. Transitions cluster phase to SHUFFLEFIX.
  3. failed Map task from COMPLETED back to IDLE
  5.Leaves the reporting Reduce task IN_PROGRESS so it retains
     ownership of all other partitions it has already downloaded locally.

  @param request ShuffleErrorReport containing failed_map_task_id,
  reduce_task_id, missing_path.
  @return wc::ShuffleErrorReply Acknowledgement flag.
 */
auto CoordinatorState::ReducePhaseShuffleError(wc::ShuffleErrorReport &request)
    -> wc::ShuffleErrorReply {
  std::int64_t map_id = request.failed_map_task_id();
  std::int64_t reduce_id = request.reduce_task_id();
  wc::ShuffleErrorReply reply;

  if (map_id < 0 || static_cast<std::size_t>(map_id) >= map_tasks.size()) {
    reply.set_acknowledged(false);
    return reply;
  }

  if (reduce_id < 0 ||
      static_cast<std::size_t>(reduce_id) >= reduce_tasks.size()) {
    reply.set_acknowledged(false);
    return reply;
  }

  job_conf.phase = Phase::SHUFFLEFIX;

  // Invalidate only the failed Map task
  if (map_tasks[map_id].task_rec.state == State::COMPLETED) {
    map_tasks[map_id].task_rec.state = State::IDLE;
    map_tasks[map_id].worker_id.clear();
    map_tasks[map_id].intermediate_op_file.clear();

    map_tasks[map_id].generation_id = curr_generation_id++;

    if (map_tasks_completed > 0) {
      map_tasks_completed--;
    }
  }

  reply.set_acknowledged(true);
  return reply;
}

// Watchdog / Reaper Loop: CheckForInactiveWorker
/**
 * @brief Periodic timer loop (runs every ~1s) to detect crashed/stalled
  workers.
 */
void CoordinatorState::CheckForInactiveWorker() {
  auto now = std::chrono::steady_clock::now();

  for (auto &task : map_tasks) {
    if (task.task_rec.state == State::IN_PROGRESS &&
        (now - task.last_heart_beat > std::chrono::seconds(10))) {
      task.task_rec.state = State::IDLE;
      task.worker_id.clear();
      task.intermediate_op_file.clear();
      task.generation_id = curr_generation_id++;
    }
  }

  for (auto &task : reduce_tasks) {
    if (task.task_rec.state == State::IN_PROGRESS &&
        (now - task.last_heart_beat > std::chrono::seconds(10))) {
      task.task_rec.state = State::IDLE;
      task.worker_id.clear();
      task.all_files_required.clear();
      task.generation_id = curr_generation_id++;
    }
  }
}

// RPC Handler: GetUpdatedReduceTaskForWorker (ShuffleErrorFix)
/**
  @brief Targeted re-attachment RPC for waiting Reduce workers during
 SHUFFLEFIX.

  Operation:
  - Called on a 500ms loop by the Reduce worker that reported a shuffle error.
  - Verifies that the caller still owns the Reduce task lease.
  - If Map tasks are still recomputing, returns TaskAssignment(WAIT).
  - Once all Map tasks are complete, re-assembles the manifest with the fresh
    URIs and returns TaskAssignment(REDUCE).
  - The Reduce worker receives the updated manifest and downloads ONLY the
    recomputed partition, preserving all previously fetched files on local
  disk.

  @param request ShuffleFixRequest containing reduce_task_id, worker_id,
  generation_id.
  @return wc::TaskAssignment WAIT or REDUCE with updated intermediate paths.
 */
auto CoordinatorState::GetUpdatedReduceTaskForWorker(
    const wc::ShuffleFixRequest &request) -> wc::TaskAssignment {

  wc::TaskAssignment task;
  auto r_id = request.reduce_task_id();

  if (r_id < 0 || static_cast<std::size_t>(r_id) >= reduce_tasks.size()) {
    task.set_type(wc::TaskType::WAIT);
    return task;
  }

  auto &r_task = reduce_tasks[r_id];

  // Verify that the calling worker owns this Reduce task
  if (r_task.worker_id != request.worker_id() ||
      r_task.generation_id != request.generation_id()) {
    task.set_type(wc::TaskType::WAIT);
    return task;
  }

  //  Map tasks are still recomputing
  if (!CheckIfMapPhaseCompleted()) {
    task.set_type(wc::TaskType::WAIT);
    return task;
  }

  task.set_type(wc::TaskType::REDUCE);
  task.set_task_id(r_id);
  task.set_generation_id(r_task.generation_id);
  task.set_num_map_tasks(job_conf.num_map_tasks);
  task.set_num_reduce_tasks(job_conf.num_reduce_partition);

  r_task.all_files_required.clear();
  for (std::size_t i = 0; i < map_tasks.size(); i++) {
    if (r_id < map_tasks[i].intermediate_op_file.size()) {
      const auto &uri = map_tasks[i].intermediate_op_file[r_id];
      task.add_intermediate_paths(uri);
      r_task.all_files_required.push_back(uri);
    }
  }

  r_task.last_heart_beat = std::chrono::steady_clock::now();
  return task;
}

/**
  @brief validates an incoming FetchInputSplit stream request
*/
auto CoordinatorState::ValidateSplitRequest(
    const wc::SplitDataRequest &request) const -> bool {
  if (request.task_id() < 0 ||
      static_cast<std::size_t>(request.task_id()) >= map_tasks.size()) {
    return false;
  }

  const auto &task = map_tasks[request.task_id()];

  return task.generation_id == request.generation_id() &&
         task.task_rec.state == State::IN_PROGRESS &&
         task.split.offset == request.offset() &&
         task.split.length == request.length();
}
} // namespace mr
