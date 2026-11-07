#pragma once

#include "mr_coordination.pb.h"
#include <optional>
namespace mr {

enum class Phase { IDLE, MAP, REDUCE, SHUFFLEFIX, DONE };
class WorkerState {
public:
  auto ExecuteMapTask(wc::TaskAssignment &task_assignment_)
      -> wc::CompleteRequest;

  auto ExecuteReduceTask(wc::TaskAssignment &task_assignment_)
      -> wc::CompleteRequest;

  Phase phase;
  std::string worker_id_;
  std::string host_;
  std::optional<wc::TaskAssignment> task_assignment_;
};
} // namespace mr