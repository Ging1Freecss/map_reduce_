#!/usr/bin/env python3
import subprocess
import time
import os
import sys
import glob
import shutil

def main():
    print("=" * 65)
    print("       MAPREDUCE SHUFFLE FAULT RECOVERY STRESS TEST")
    print("=" * 65)

    binaries = [
        "./build/coordinator/coordinator",
        "./build/worker/worker",
        "./build/tests/sequential_harness"
    ]
    for b in binaries:
        if not os.path.exists(b):
            print(f"Error: {b} not found. Run: cmake --build build -j$(nproc)")
            sys.exit(1)

    # 1. Clean workspaces
    print("[1/5] Cleaning intermediate directories...")
    for d in [
        "/var/tmp/mr_worker",
        "/tmp/mr_stress_test"
    ]:
        if os.path.exists(d):
            shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d, exist_ok=True)

    # 2. Generate ~40,000 lines of data (creates ~8 Map tasks with 40KB splits)
    input_file = "/tmp/mr_stress_test/stress_input.txt"
    print("[2/5] Creating stress input dataset (40,000 lines)...")
    words = "apple banana cherry date elderberry fig grape honeydew kiwi lemon\n"
    with open(input_file, "w") as f:
        for _ in range(40000):
            f.write(words)

    # 3. Generate Sequential Ground Truth
    print("[3/5] Generating golden reference with sequential_harness...")
    golden_dir = "/tmp/mr_stress_test/golden"
    os.makedirs(golden_dir, exist_ok=True)
    subprocess.run(
        ["./build/tests/sequential_harness", input_file, "-r", "2", "-w", golden_dir],
        check=True, stdout=subprocess.DEVNULL
    )
    golden_files = sorted(glob.glob(f"{golden_dir}/mr-out-*"), key=lambda p: os.path.basename(p))
    golden_sorted = "/tmp/mr_stress_test/golden_sorted.txt"
    os.system(f"cat {' '.join(golden_files)} > {golden_sorted}")

    # 4. Start Coordinator and 4 Workers (W=4 > R=2)
    print("[4/5] Launching Coordinator and 4 Workers (:50052, :50053, :50054, :50055)...")
    log_dir = "/tmp/mr_stress_test/logs"
    os.makedirs(log_dir, exist_ok=True)

    coord_log = open(f"{log_dir}/coordinator.log", "w")
    w1_log = open(f"{log_dir}/w1.log", "w")
    w2_log = open(f"{log_dir}/w2.log", "w")
    w3_log = open(f"{log_dir}/w3.log", "w")
    w4_log = open(f"{log_dir}/w4.log", "w")

    # Split size 40000 ensures ~8-10 map tasks
    coord = subprocess.Popen(
        ["./build/coordinator/coordinator", input_file, "-p", "50051", "-r", "2", "-s", "40000"],
        stdout=coord_log, stderr=subprocess.STDOUT
    )
    time.sleep(0.3)

    w1 = subprocess.Popen(["./build/worker/worker", "-c", "127.0.0.1:50051", "-p", "50052", "-h", "127.0.0.1"], stdout=w1_log, stderr=subprocess.STDOUT)
    w2 = subprocess.Popen(["./build/worker/worker", "-c", "127.0.0.1:50051", "-p", "50053", "-h", "127.0.0.1"], stdout=w2_log, stderr=subprocess.STDOUT)
    w3 = subprocess.Popen(["./build/worker/worker", "-c", "127.0.0.1:50051", "-p", "50054", "-h", "127.0.0.1"], stdout=w3_log, stderr=subprocess.STDOUT)
    w4 = subprocess.Popen(["./build/worker/worker", "-c", "127.0.0.1:50051", "-p", "50055", "-h", "127.0.0.1"], stdout=w4_log, stderr=subprocess.STDOUT)

    # 5. Fault Injection: Wait until Worker 2 produces a map shard, then kill it
    print("[5/5] Monitoring intermediate shards for fault injection...")
    fault_injected = False
    for _ in range(200):
        time.sleep(0.05)
        # Check if Worker 2 (:50053) produced intermediate shards
        shards = glob.glob(f"/var/tmp/mr_worker/*50053*/intermediate/*.data")
        if not shards:
            shards = glob.glob(f"/var/tmp/mr_worker/*{w2.pid}*/intermediate/*.data")
        if not shards:
            shards = glob.glob("/var/tmp/mr_worker/*/intermediate/*.data")
        if shards:
            print(f"       -> 💥 Terminating Worker 2 (:50053) mid-job! Detected shards: {[os.path.basename(x) for x in shards[:3]]}")
            w2.kill()
            w2.wait()
            # Delete Worker 2's produced shards so reducers are forced to trigger SHUFFLEFIX
            for f in shards:
                try: os.remove(f)
                except: pass
            fault_injected = True
            break

    if not fault_injected:
        print("Failed to inject fault in time.")
        sys.exit(1)

    print("Waiting for cluster recovery and job completion...")
    time.sleep(14)

    for p in [w1, w3, w4, coord]:
        try: p.terminate(); p.wait(timeout=1)
        except:
            try: p.kill()
            except: pass

    coord_log.close(); w1_log.close(); w2_log.close(); w3_log.close(); w4_log.close()

    # 6. Verify Correctness & Assertions
    cluster_files = sorted(glob.glob("/var/tmp/mr_worker/*/reduce_output/mr-out-*"), key=lambda p: os.path.basename(p))
    cluster_sorted = "/tmp/mr_stress_test/cluster_sorted.txt"
    os.system(f"cat {' '.join(cluster_files)} > {cluster_sorted}")

    diff_res = subprocess.run(["diff", "-u", golden_sorted, cluster_sorted], capture_output=True, text=True)

    with open(f"{log_dir}/coordinator.log") as f:
        coord_content = f.read()

    shuffle_recovered = "Shuffle Fix for" in coord_content

    print("\n" + "=" * 65)
    print("                 STRESS TEST EXECUTION AUDIT")
    print("=" * 65)
    print(f"1. Zero Data Loss (Ground Truth Diff Match): {'PASSED' if diff_res.returncode == 0 else 'FAILED'}")
    print(f"2. Actual Shuffle Fix Triggered:             {'PASSED' if shuffle_recovered else 'FAILED (False Positive!)'}")
    print("=" * 65)

    if diff_res.returncode == 0 and shuffle_recovered:
        print("🎉 STRESS TEST PASSED: Fault successfully injected, escalated, recomputed, and verified!")
        sys.exit(0)
    else:
        print("❌ STRESS TEST FAILED!")
        if diff_res.returncode != 0:
            print(diff_res.stdout)
        sys.exit(1)

if __name__ == "__main__":
    main()
