#!/usr/bin/env python3
import subprocess
import time
import os
import sys
import glob
import shutil

def print_log_summary(log_dir):
    print("\n" + "=" * 60)
    print("                 DETAILED EXECUTION LOGS")
    print("=" * 60)

    # 1. Coordinator Log
    coord_path = os.path.join(log_dir, "coordinator.log")
    if os.path.exists(coord_path):
        print("\n--- [Coordinator] Orchestration & ShuffleFix Timeline ---")
        with open(coord_path) as f:
            for line in f:
                line_str = line.strip()
                if any(k in line_str for k in ["Starting", "PollTask", "CompleteTask", "Shuffle Fix", "Shutdown"]):
                    print("  " + line_str)

    # 2. Worker Logs
    for w_name in ["worker1.log", "worker2.log", "worker3.log"]:
        w_path = os.path.join(log_dir, w_name)
        if os.path.exists(w_path):
            title = w_name.replace(".log", "").upper()
            print(f"\n--- [{title}] Key Events ---")
            with open(w_path) as f:
                for line in f:
                    line_str = line.strip()
                    if any(k in line_str for k in ["Assigned", "Failed to download", "Succeed to download", "Reporting shuffle error", "completed", "Hard-linked"]):
                        print("  " + line_str)

    # 3. Final Output Preview
    cluster_sorted = "/tmp/mr_recovery_test/cluster_sorted.txt"
    if os.path.exists(cluster_sorted):
        print("\n--- Final Aggregated WordCount Output ---")
        with open(cluster_sorted) as f:
            for line in f:
                print("  " + line.strip())
    print("=" * 60)

def main():
    print("=" * 60)
    print("       MAPREDUCE SHUFFLE FAULT RECOVERY TEST")
    print("=" * 60)

    # 1. Verify Binaries
    binaries = [
        "./build/coordinator/coordinator",
        "./build/worker/worker",
        "./build/tests/sequential_harness"
    ]
    for b in binaries:
        if not os.path.exists(b):
            print(f"Error: Required binary not found: {b}")
            print("Please build the project first: cmake --build build -j$(nproc)")
            sys.exit(1)

    # 2. Clean Temporary Directories
    print("[1/5] Cleaning /var/tmp directories and test workspace...")
    temp_dirs = [
        "/var/tmp/mr_worker",
        "/tmp/mr_recovery_test"
    ]
    for d in temp_dirs:
        if os.path.exists(d):
            shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d, exist_ok=True)

    # Prepare input file (multi-line to ensure clean splits)
    input_file = "/tmp/mr_recovery_test/test_input.txt"
    with open(input_file, "w") as f:
        for _ in range(500):
            f.write("apple banana cherry date apple banana\n")

    # 3. Generate Ground Truth with Sequential Harness
    print("[2/5] Generating golden reference via sequential_harness...")
    golden_dir = "/tmp/mr_recovery_test/golden"
    os.makedirs(golden_dir, exist_ok=True)
    res_seq = subprocess.run(
        ["./build/tests/sequential_harness", input_file, "-r", "2", "-w", golden_dir],
        capture_output=True, text=True
    )
    if res_seq.returncode != 0:
        print("Error: sequential_harness failed:")
        print(res_seq.stderr)
        sys.exit(1)

    golden_files = sorted(glob.glob(f"{golden_dir}/mr-out-*"), key=lambda p: os.path.basename(p))
    golden_sorted = "/tmp/mr_recovery_test/golden_sorted.txt"
    os.system(f"cat {' '.join(golden_files)} > {golden_sorted}")

    # 4. Start Coordinator and Initial 2 Workers (50052, 50053)
    print("[3/5] Starting Coordinator and initial Workers 1 & 2...")
    log_dir = "/tmp/mr_recovery_test/logs"
    os.makedirs(log_dir, exist_ok=True)
    
    coord_log = open(f"{log_dir}/coordinator.log", "w")
    w1_log = open(f"{log_dir}/worker1.log", "w")
    w2_log = open(f"{log_dir}/worker2.log", "w")
    w3_log = open(f"{log_dir}/worker3.log", "w")

    # Target split size 4000 creates ~5 map tasks
    coord = subprocess.Popen(
        ["./build/coordinator/coordinator", input_file, "-p", "50051", "-r", "2", "-s", "4000"],
        stdout=coord_log, stderr=subprocess.STDOUT
    )
    time.sleep(0.5)

    w1 = subprocess.Popen(
        ["./build/worker/worker", "-c", "127.0.0.1:50051", "-p", "50052", "-h", "127.0.0.1"],
        stdout=w1_log, stderr=subprocess.STDOUT
    )
    w2 = subprocess.Popen(
        ["./build/worker/worker", "-c", "127.0.0.1:50051", "-p", "50053", "-h", "127.0.0.1"],
        stdout=w2_log, stderr=subprocess.STDOUT
    )

    # 5. Fault Injection: Wait until Map files exist, kill Worker 2 and spawn Worker 3
    print("[4/5] Injecting Fault: Monitoring intermediate files to trigger missing shard...")
    for _ in range(100):
        time.sleep(0.05)
        # Once Worker 2 finishes at least one map task, kill Worker 2 so its port goes down!
        shards = glob.glob("/var/tmp/mr_worker/*50053*/intermediate/*.data")
        if not shards:
            shards = glob.glob(f"/var/tmp/mr_worker/*{w2.pid}*/intermediate/*.data")
        if not shards:
            shards = glob.glob("/var/tmp/mr_worker/*/intermediate/*.data")
        if shards:
            time.sleep(0.05)
            print("       -> 💥 FAULT INJECTED: Terminating Worker 2 (:50053) to simulate peer crash!")
            w2.kill()
            w2.wait()
            # Also purge Worker 2's intermediate shards from disk so peer fetch fails
            for f in shards:
                try: os.remove(f)
                except OSError: pass
            break

    # Spawn Worker 3 as the spare worker to pick up recomputed tasks
    print("       -> Spawning Worker 3 (:50054) as the spare recovery worker...")
    w3 = subprocess.Popen(
        ["./build/worker/worker", "-c", "127.0.0.1:50051", "-p", "50054", "-h", "127.0.0.1"],
        stdout=w3_log, stderr=subprocess.STDOUT
    )

    # Wait for cluster to detect fault, escalate, recompute, and finish reduce
    print("[5/5] Waiting for SHUFFLEFIX recovery and Reduce phase completion...")
    time.sleep(7)

    for p in [w1, w3, coord]:
        try:
            p.terminate()
            p.wait(timeout=1)
        except Exception:
            try: p.kill()
            except Exception: pass

    coord_log.close(); w1_log.close(); w2_log.close(); w3_log.close()

    # 6. Verify Output
    cluster_out_files = sorted(glob.glob("/var/tmp/mr_worker/*/reduce_output/mr-out-*"), key=lambda p: os.path.basename(p))
    if not cluster_out_files:
        print("\n❌ TEST FAILED: No output files produced in /var/tmp/mr_worker/*/reduce_output!")
        print_log_summary(log_dir)
        sys.exit(1)

    cluster_sorted = "/tmp/mr_recovery_test/cluster_sorted.txt"
    os.system(f"cat {' '.join(cluster_out_files)} > {cluster_sorted}")

    diff_res = subprocess.run(["diff", "-u", golden_sorted, cluster_sorted], capture_output=True, text=True)

    # 7. Print summary and logs
    print_log_summary(log_dir)

    if diff_res.returncode == 0:
        print("\n🎉 TEST PASSED: Full recovery succeeded with ZERO data loss!")
        print(f"Produced partitions: {sorted(cluster_out_files)}")
        print("All word counts match the sequential ground truth reference.")
        print(f"Raw log files available at: {log_dir}")
        print("=" * 60)
        sys.exit(0)
    else:
        print("\n❌ TEST FAILED: Output mismatch!")
        print(diff_res.stdout)
        print(f"Raw log files available at: {log_dir}")
        print("=" * 60)
        sys.exit(1)

if __name__ == "__main__":
    main()
