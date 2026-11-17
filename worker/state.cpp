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

auto WorkerState::ValidateParseP2pUri(const std::string &uri,
                                      std::int64_t expected_reducer_id,
                                      std::int32_t num_map_tasks)
    -> std::expected<ParsedPartitionUri, std::string> {

  // split on '/'
  const auto slash_pos = uri.find('/');

  if (slash_pos == std::string::npos || slash_pos == 0 ||
      slash_pos == uri.size() - 1) {
    return std::unexpected("Malformed URI: missing or invalid '/' in: " + uri);
  }

  std::string endpoint = uri.substr(0, slash_pos);
  std::string filename = uri.substr(slash_pos + 1);

  // validate endpoint format "host:port"
  const auto colon_pos = endpoint.rfind(':');

  if (colon_pos == std::string::npos || colon_pos == 0 ||
      colon_pos == endpoint.size() - 1) {
    return std::unexpected("Malformed endpoint (missing host:port): " +
                           endpoint);
  }

  int port = 0;

  // check port
  for (std::size_t i = colon_pos + 1; i < endpoint.size(); i++) {

    char c = endpoint[i];

    if (c < '0' || c > '9') {
      return std::unexpected("Non-digit character found in port: " + endpoint);
    }

    port = port * 10 + (c - '0');

    //  out-of-range port
    if (port > 65535) {
      return std::unexpected("Port number exceeds 65535 in endpoint: " +
                             endpoint);
    }
  }

  // port 0 is reserved/invalid
  if (port <= 0) {
    return std::unexpected("Port number must be > 0 in endpoint: " + endpoint);
  }

  std::filesystem::path file_path{filename};
  if (file_path.filename().string() != filename || filename == "." ||
      filename == "..") {
    return std::unexpected("Security violation: path traversal in filename: " +
                           filename);
  }

  // minimum valid length: "mr-0_0.data" is 11 chars
  if (filename.size() < 11 || !filename.starts_with("mr-") ||
      !filename.ends_with(".data")) {
    return std::unexpected(
        "Filename does not match 'mr-<map>_<reduce>.data': " + filename);
  }

  const std::size_t prefix_len = 3; // length of "mr-"
  const std::size_t suffix_len = 5; // length of ".data"
  const std::size_t end_of_digits = filename.size() - suffix_len;

  const auto underscore_pos = filename.find('_', prefix_len);

  if (underscore_pos == std::string::npos || underscore_pos == prefix_len ||
      underscore_pos >= end_of_digits - 1) {
    return std::unexpected(
        "Malformed partition filename (missing or misplaced '_'): " + filename);
  }

  //  map_id (characters between index 3 and underscore_pos)
  std::int64_t map_id = 0;
  for (std::size_t i = prefix_len; i < underscore_pos; ++i) {
    char c = filename[i];
    if (c < '0' || c > '9') {
      return std::unexpected("Non-digit in map_id of filename: " + filename);
    }
    map_id = map_id * 10 + (c - '0');
  }

  // Parse reduce_id (characters between underscore_pos + 1 and
  // end_of_digits)
  std::int64_t reduce_id = 0;
  for (std::size_t i = underscore_pos + 1; i < end_of_digits; ++i) {
    char c = filename[i];
    if (c < '0' || c > '9') {
      return std::unexpected("Non-digit in reduce_id of filename: " + filename);
    }
    reduce_id = reduce_id * 10 + (c - '0');
  }

  if (reduce_id != expected_reducer_id) {
    return std::unexpected("Partition " + filename + " belongs to reducer " +
                           std::to_string(reduce_id) + ", expected reducer " +
                           std::to_string(expected_reducer_id));
  }
  if (num_map_tasks > 0 && (map_id < 0 || map_id >= num_map_tasks)) {
    return std::unexpected("Map task ID " + std::to_string(map_id) +
                           " out of range [0, " +
                           std::to_string(num_map_tasks) + ")");
  }

  return ParsedPartitionUri{
      .peer_endpoint = std::move(endpoint),
      .filename = std::move(filename),
      .map_id = map_id,
      .reduce_id = reduce_id,
  };
}
} // namespace mr