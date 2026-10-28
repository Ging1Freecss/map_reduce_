#include "mr_coordination.grpc.pb.h"
#include "mr_coordination.pb.h"
#include <cstdint>
namespace mr {
enum class Phase { MAP, REDUCE, DONE };

struct TaskRecord {
  std::int64_t task_id;
  wc::TaskType type;
  enum class State { IDLE, IN_PROGRESS, COMPLETED } state{State::IDLE};

};
} // namespace mr