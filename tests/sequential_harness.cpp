#include "engine/sequential_harness.hpp"
#include "storage/error.hpp"
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>

void print_usage(const char *prog) {
  std::cout
      << "Usage: " << prog << " [options] <input_file>\n"
      << "Options:\n"
      << "  --reducers, -r <N>   Number of reduce partitions (default: 4)\n"
      << "  --workdir,  -w <dir> Staging directory for intermediate and output "
         "files (default: ./mr_output)\n"
      << "  --help,     -h       Print this help message\n\n"
      << "Example:\n"
      << "  " << prog << " data/sample.txt -r 4 -w ./mr_output\n";
}

int main(int argc, char *argv[]) {
  std::filesystem::path input_file;
  std::filesystem::path work_dir = "./mr_output";
  std::size_t num_reducers = 4;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      print_usage(argv[0]);
      return 0;
    } else if ((arg == "--reducers" || arg == "-r") && i + 1 < argc) {
      num_reducers = std::stoul(argv[++i]);
    } else if ((arg == "--workdir" || arg == "-w") && i + 1 < argc) {
      work_dir = argv[++i];
    } else if (arg.starts_with("-")) {
      std::cerr << "Unknown option: " << arg << "\n";
      print_usage(argv[0]);
      return 1;
    } else {
      input_file = arg;
    }
  }

  if (input_file.empty()) {
    std::cerr << "Error: No input file specified.\n";
    print_usage(argv[0]);
    return 1;
  }

  if (!std::filesystem::exists(input_file)) {
    std::cerr << "Error: Input file does not exist: " << input_file << "\n";
    return 1;
  }

  std::cout << "========================================================\n"
            << "       MAPREDUCE SEQUENTIAL HARNESS (PHASE 2)          \n"
            << "========================================================\n"
            << "Input File:     " << input_file << "\n"
            << "Partitions (R): " << num_reducers << "\n"
            << "Working Dir:    " << work_dir << "\n"
            << "--------------------------------------------------------\n";

  auto start = std::chrono::steady_clock::now();

  auto res = mr::run_sequential_map_reduce(input_file, work_dir, num_reducers);
  if (!res) {
    std::cerr << "MapReduce failed with error: "
              << mr::storage_erro_to_string(res.error()) << "\n";
    return 1;
  }

  auto end = std::chrono::steady_clock::now();
  auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count();

  std::cout << "SUCCESS: Pipeline completed in " << elapsed_ms << " ms\n"
            << "Intermediate files: " << work_dir << "/mr-0_<r>.data\n"
            << "Output partitions:  " << work_dir << "/mr-out-<r>\n\n"
            << "To generate golden reference:\n"
            << "  cat " << work_dir
            << "/mr-out-* | sort -k1,1 > golden_reference.txt\n"
            << "========================================================\n";

  return 0;
}
