#pragma once

#include "mr_coordination.pb.h"
#include <cstdint>
#include <expected>
#include <filesystem>
#include <grpcpp/support/status.h>
#include <optional>
#include <string>
namespace mr {

enum class Phase { IDLE, MAP, REDUCE, SHUFFLEFIX, DONE };

// in reduce phase when downloading data from map workers
struct ParsedPartitionUri {
  std::string peer_endpoint;
  std::string filename;
  std::optional<std::int64_t> map_id{std::nullopt};
  std::optional<std::int64_t> reduce_id{std::nullopt};
};

struct MapPhaseOutputFilesStatus {
  ParsedPartitionUri data;
  grpc::Status status;
};

class WorkerState {
public:
  WorkerState();

  void InitDirectories(const std::string &base_dir = "/var/tmp/mr_worker");

  auto ExecuteMapTask() -> std::optional<wc::CompleteRequest>;

  auto GetMapTaskInputFileName(const std::string &input_file,
                               std::int64_t task_id) -> std::string;

  void ResetWorkerState();

  void SetState(wc::TaskAssignment task, Phase phase);

  auto ValidateParseP2pUri(const std::string &uri,
                           std::int64_t expected_reducer_id,
                           std::int32_t num_map_tasks)
      -> std::expected<ParsedPartitionUri, std::string>;

  auto ExecuteReduceTask(const std::vector<std::filesystem::path> &file_path)
      -> std::optional<wc::CompleteRequest>;
  auto ReduceOutputFileName(std::size_t reduce_id) -> std::string;

  Phase phase_{Phase::IDLE};
  std::string worker_id_;
  std::string advertised_host_;
  std::optional<wc::TaskAssignment> task_assignment_{std::nullopt};

  std::string map_phase_op_dir;
  std::string reduce_phase_ip_dir;
  std::string reduce_phase_out_dir;
  std::optional<std::string> output_reduce_path_{std::nullopt};
};
} // namespace mr