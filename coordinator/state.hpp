#include "mr_coordination.pb.h"
#include "storage/split_reader.hpp"
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <vector>
namespace mr {
enum class Phase { CONFIGURE, MAP, REDUCE, SHUFFLEFIX, DONE };
enum class State { IDLE, IN_PROGRESS, COMPLETED };

struct TaskRecord {
  std::int64_t task_id;
  wc::TaskType type;
  State state;
};

struct JobConfiguration {
  std::filesystem::path input_path;
  std::size_t num_map_tasks; // equals to number of splits of input data
  std::size_t num_reduce_partition;
  Phase phase;
  std::uint64_t target_split_bytes;
};

// coodinator have an array of MapTask
struct MapTask {
  TaskRecord task_rec;
  InputSplit split;

  /*
    when worker poll for task,
    only then we assign generation id and worker id
    worker id :"worker-" + get_hostname() + "-" + std::to_string(getpid());
     std::string host = boost::asio::ip::host_name(); for host name
    we assign last_heart_beat when worker poll for task or
    when heartbeat
  */
  std::uint64_t generation_id{0};
  std::string worker_id{""};
  std::chrono::steady_clock::time_point last_heart_beat;

  // for worker in reduce phase, that will use data from this map task
  /*
  intermediate_uris = [
  "10.0.0.1:50052/mr-0_0.data",   // For Reducer 0
  "10.0.0.1:50052/mr-0_1.data",   // For Reducer 1
  "10.0.0.1:50052/mr-0_2.data",   // For Reducer 2
  "10.0.0.1:50052/mr-0_3.data"    // For Reducer 3
]

  files name should look like this, where index refer to reducer task id
  */
  std::vector<std::string> intermediate_op_file;
};

// coordinator has an array of reduce task
struct ReduceTask {
  TaskRecord task_rec;
  std::uint64_t generation_id;
  std::string worker_id;
  std::chrono::steady_clock::time_point last_heart_beat;
  std::vector<std::string> all_files_required;
  std::string output_path;
};

class CoordinatorState {
private:
  std::vector<MapTask> map_tasks;
  std::vector<ReduceTask> reduce_tasks;
  std::atomic<std::uint64_t> curr_generation_id;
  std::size_t map_tasks_completed{0};
  std::size_t reduce_tasks_completed{0};

public:
  JobConfiguration job_conf;

  CoordinatorState(std::filesystem::path input_path,
                   std::size_t reduce_partition,
                   std::uint64_t target_split_bytes = 64 * 1024 * 1024);

  auto DistributeTaskToPollingWorker(std::string worker_id)
      -> wc::TaskAssignment;

  auto WorkerReportCompletedMapTask(const wc::CompleteRequest &request)
      -> wc::CompleteReply;

  auto CheckLastHeartBeat(wc::HeartBeatRequest &request) -> wc::HeartBeatReply;

  auto ReducePhaseShuffleError(wc::ShuffleErrorReport &request)
      -> wc::ShuffleErrorReply;

  auto GetUpdatedReduceTaskForWorker(const wc::ShuffleFixRequest &request)
      -> wc::TaskAssignment;

  auto ValidateSplitRequest(const wc::SplitDataRequest &request) const -> bool;

  auto CheckIfMapPhaseCompleted() -> bool;

  auto CheckIfReducePhaseCompleted() -> bool;

  void CheckForInactiveWorker();
};
} // namespace mr