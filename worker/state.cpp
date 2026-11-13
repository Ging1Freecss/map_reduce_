#include "state.hpp"
#include "engine/map_emit_context.hpp"
#include "mr_coordination.pb.h"
#include <fstream>
#include <optional>
#include <string>

namespace mr {
auto WorkerState::ExecuteMapTask() -> std::optional<wc::CompleteRequest> {

  if (!task_assignment_) {
    return std::nullopt;
  }

  wc::TaskAssignment &task = task_assignment_.value();

  // input file name downloaded by fetch map
  const std::string map_input_file{
      GetMapTaskInputFileName(task.input_file(), task.task_id())};
  const std::size_t map_id = static_cast<std::size_t>(task.task_id());
  const std::size_t num_reducers =
      static_cast<std::size_t>(task.num_reduce_tasks());

  std::ifstream in{map_input_file};

  if (!in.is_open()) {
    return std::nullopt;
  }

  // local directory for output of map phase
  std::filesystem::path local_base_dir = map_phase_op_dir;
  std::error_code ec;
  std::filesystem::create_directories(local_base_dir, ec);

  if (ec)
    return std::nullopt;

  // this parse the data to key value pair -> {"apple",1}
  MapEmitContext<HashPartitioner> ctx{map_id, num_reducers, local_base_dir};

  std::string line;
  std::size_t offset{0};
  WordCountMapper mapper;

  while (std::getline(in, line)) {
    // parse line -> separate word -> emit to respective intermediate file
    // like {"apple",1}
    auto res = mapper.map(std::to_string(offset), line, ctx);

    if (!res)
      return std::nullopt;

    offset += line.length() + 1;
  }

  auto commit_res = ctx.commit();
  if (!commit_res) {
    return std::nullopt;
  }

  wc::CompleteRequest request;
  request.set_task_id(task.task_id());
  request.set_generation_id(task.generation_id());
  request.set_type(wc::TaskType::MAP);
  // Host IP and port (e.g., "10.0.0.1:50052") are prefixed here so other
  // workers can connect:
  for (std::size_t i = 0; i < num_reducers; ++i) {
    std::string filename =
        "mr-" + std::to_string(map_id) + "_" + std::to_string(i) + ".data";
    std::string p2p_uri = advertised_host_ + "/" + filename;
    request.add_output_paths(std::move(p2p_uri));
  }

  return request;
}

auto WorkerState::GetMapTaskInputFileName(const std::string &input_file,
                                          std::int64_t task_id) -> std::string {
  return std::string{input_file + ".map_data." + std::to_string(task_id)};
}

void WorkerState::ResetWorkerState() {
  phase_ = Phase::IDLE;
  task_assignment_ = std::nullopt;
}

void WorkerState::SetMapState(wc::TaskAssignment task) {
  phase_ = Phase::MAP;
  task_assignment_ = std::move(task);
}
} // namespace mr