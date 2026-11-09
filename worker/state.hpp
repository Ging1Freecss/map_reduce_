#pragma once

#include "mr_coordination.pb.h"
#include <cstdint>
#include <optional>
namespace mr {

enum class Phase { IDLE, MAP, REDUCE, SHUFFLEFIX, DONE };
class WorkerState {
public:
  auto ExecuteMapTask() -> std::optional<wc::CompleteRequest>;

  auto GetMapTaskInputFileName(const std::string &input_file,
                               std::int64_t task_id) -> std::string;

  void ResetWorkerState();

  void SetMapState(wc::TaskAssignment task);
  auto ExecuteReduceTask(wc::TaskAssignment &task_assignment_)
      -> wc::CompleteRequest;

  Phase phase_{Phase::IDLE};
  std::string worker_id_;
  std::string advertised_host_;
  std::optional<wc::TaskAssignment> task_assignment_{std::nullopt};
};
} // namespace mr