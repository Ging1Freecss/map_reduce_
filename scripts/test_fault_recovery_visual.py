#!/usr/bin/env python3
import subprocess
import time
import os
import sys
import glob
import shutil
import struct

# ANSI Colors for crystal-clear terminal visibility
RESET = "\033[0m"
BOLD = "\033[1m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"
CYAN = "\033[96m"

def print_banner(text, color=CYAN):
    print("\n" + color + BOLD + "=" * 70 + RESET)
    print(color + BOLD + f"  {text}" + RESET)
    print(color + BOLD + "=" * 70 + RESET)

def main():
    print_banner("MAPREDUCE SHUFFLE FAULT INJECTION & RECOVERY DEMO", CYAN)

    binaries = [
        "./build/coordinator/coordinator",
        "./build/worker/worker",
        "./build/tests/sequential_harness"
    ]
    for b in binaries:
        if not os.path.exists(b):
            print(f"{RED}Error: Required binary not found: {b}{RESET}")
            print("Please run: cmake --build build -j$(nproc)")
            sys.exit(1)

    # 1. Clean Directories
    print(f"\n{BOLD}[Step 1] Cleaning work directories...{RESET}")
    for d in [
        "/var/tmp/mr_worker",
        "/tmp/mr_demo_test"
    ]:
        if os.path.exists(d):
            shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d, exist_ok=True)

    # 2. Prepare Input File
    print(f"{BOLD}[Step 2] Generating input data (30,000 lines)...{RESET}")
    input_file = "/tmp/mr_demo_test/demo_input.txt"
    words = "alpha beta gamma delta epsilon zeta eta theta iota kappa\n"
    with open(input_file, "w") as f:
        for _ in range(30000):
            f.write(words)

    # 3. Generate Ground Truth
    print(f"{BOLD}[Step 3] Running sequential_harness to generate ground truth...{RESET}")
    golden_dir = "/tmp/mr_demo_test/golden"
    os.makedirs(golden_dir, exist_ok=True)
    subprocess.run(
        ["./build/tests/sequential_harness", input_file, "-r", "2", "-w", golden_dir],
        check=True, stdout=subprocess.DEVNULL
    )
    golden_sorted = "/tmp/mr_demo_test/golden_sorted.txt"
    os.system(f"cat {golden_dir}/mr-out-* | sort > {golden_sorted}")

    # 4. Start Cluster
    print_banner("STEP 4: STARTING MAPREDUCE CLUSTER", BLUE)
    print(f"  • Coordinator:  0.0.0.0:50051 (2 Reducers, 40KB Splits)")
    print(f"  • Worker 1:     127.0.0.1:50052 (Active worker)")
    print(f"  • Worker 2:     127.0.0.1:50053 ({RED}Target for Fault Injection{RESET})")
    print(f"  • Worker 3:     127.0.0.1:50054 (Active worker)")
    print(f"  • Worker 4:     127.0.0.1:50055 (Spare worker for recovery)")

    log_dir = "/tmp/mr_demo_test/logs"
    os.makedirs(log_dir, exist_ok=True)

    coord_log = open(f"{log_dir}/coordinator.log", "w")
    w1_log = open(f"{log_dir}/w1.log", "w")
    w2_log = open(f"{log_dir}/w2.log", "w")
    w3_log = open(f"{log_dir}/w3.log", "w")
    w4_log = open(f"{log_dir}/w4.log", "w")

    coord = subprocess.Popen(["./build/coordinator/coordinator", input_file, "-p", "50051", "-r", "2", "-s", "40000"], stdout=coord_log, stderr=subprocess.STDOUT)
    time.sleep(0.3)

    w1 = subprocess.Popen(["./build/worker/worker", "-c", "127.0.0.1:50051", "-p", "50052", "-h", "127.0.0.1"], stdout=w1_log, stderr=subprocess.STDOUT)
    w2 = subprocess.Popen(["./build/worker/worker", "-c", "127.0.0.1:50051", "-p", "50053", "-h", "127.0.0.1"], stdout=w2_log, stderr=subprocess.STDOUT)
    w3 = subprocess.Popen(["./build/worker/worker", "-c", "127.0.0.1:50051", "-p", "50054", "-h", "127.0.0.1"], stdout=w3_log, stderr=subprocess.STDOUT)
    w4 = subprocess.Popen(["./build/worker/worker", "-c", "127.0.0.1:50051", "-p", "50055", "-h", "127.0.0.1"], stdout=w4_log, stderr=subprocess.STDOUT)

    # 5. Fault Injection: Wait until Worker 2 produces shards, then terminate it
    print_banner("STEP 5: INJECTING NODE CRASH", RED)
    victim_killed = False
    killed_shards = []
    for _ in range(150):
        time.sleep(0.05)
        # Check if Worker 2 (:50053) produced intermediate shards
        shards = glob.glob(f"/var/tmp/mr_worker/*50053*/intermediate/*.data")
        if not shards:
            shards = glob.glob(f"/var/tmp/mr_worker/*{w2.pid}*/intermediate/*.data")
        if not shards:
            shards = glob.glob("/var/tmp/mr_worker/*/intermediate/*.data")
        if shards:
            print(f"{RED}{BOLD}💥 CRASH INJECTED!{RESET} Terminating Worker 2 (:50053)...")
            w2.kill()
            w2.wait()
            # Delete shards produced by worker 2 so reducers can never download them
            killed_shards = shards
            for f in shards:
                try: os.remove(f)
                except: pass
            print(f"   Purged intermediate partitions from disk: {[os.path.basename(x) for x in killed_shards[:3]]}")
            victim_killed = True
            break

    if not victim_killed:
        print("Failed to detect shards in time.")
        sys.exit(1)

    print(f"\n{YELLOW}{BOLD}Waiting for failure detection, SHUFFLEFIX escalation, recomputation, and completion...{RESET}")
    time.sleep(12)

    # Clean shutdown
    for p in [w1, w3, w4, coord]:
        try: p.terminate(); p.wait(timeout=1)
        except:
            try: p.kill()
            except: pass

    coord_log.close(); w1_log.close(); w2_log.close(); w3_log.close(); w4_log.close()

    # 6. Parse and Display the Step-by-Step Failure & Recovery Timeline
    print_banner("STEP 6: STEP-BY-STEP FAILURE & RECOVERY AUDIT TIMELINE", GREEN)

    print(f"{BOLD}1. Failure Detection by Reducer:{RESET}")
    with open(f"{log_dir}/w1.log") as f:
        w1_lines = f.readlines()
    with open(f"{log_dir}/w3.log") as f:
        w3_lines = f.readlines()
    all_worker_lines = w1_lines + w3_lines

    detected = [l.strip() for l in all_worker_lines if "Failed to download partition" in l or "Escalating shuffle failure" in l]
    for d in detected[:4]:
        print(f"   {RED}{d}{RESET}")

    print(f"\n{BOLD}2. Coordinator Receives Error & Transitions to SHUFFLEFIX:{RESET}")
    with open(f"{log_dir}/coordinator.log") as f:
        coord_lines = f.readlines()

    coord_fault_events = [l.strip() for l in coord_lines if "ShuffleErrorReport received" in l or "Shuffle Fix for" in l]
    for c in coord_fault_events:
        if "ShuffleErrorReport" in c:
            print(f"   {YELLOW}{c}{RESET}")
        elif "Recomputation complete" in c or "Assigning updated Reduce Task" in c:
            print(f"   {GREEN}{c}{RESET}")
        elif "Still Waiting" in c:
            # print first 2 waiting messages
            pass

    print(f"\n{BOLD}3. Spare Worker Recomputes the Missing Map Task:{RESET}")
    with open(f"{log_dir}/w4.log") as f:
        w4_lines = f.readlines()
    w4_maps = [l.strip() for l in w4_lines if "Assigned MAP task" in l or "completed and accepted" in l]
    for m in w4_maps:
        print(f"   {CYAN}{m}{RESET}")

    print(f"\n{BOLD}4. Reducer Re-attachment & Completion:{RESET}")
    reducer_success = [l.strip() for l in all_worker_lines if "Recomputation complete" in l or "Reduce phase completed" in l or "Succeed to download" in l]
    for s in reducer_success[-4:]:
        print(f"   {GREEN}{s}{RESET}")

    # 7. Ground Truth Diff Verification
    cluster_files = sorted(glob.glob("/var/tmp/mr_worker/*/reduce_output/mr-out-*"), key=lambda p: os.path.basename(p))
    golden_files = sorted(glob.glob(f"{golden_dir}/mr-out-*"), key=lambda p: os.path.basename(p))

    cluster_sorted = "/tmp/mr_demo_test/cluster_sorted.txt"
    os.system(f"cat {' '.join(cluster_files)} > {cluster_sorted}")
    os.system(f"cat {' '.join(golden_files)} > {golden_sorted}")

    diff_res = subprocess.run(["diff", "-u", golden_sorted, cluster_sorted], capture_output=True, text=True)

    print_banner("VERIFICATION & SUMMARY", GREEN if diff_res.returncode == 0 else RED)
    if diff_res.returncode == 0:
        print(f"{GREEN}{BOLD}🎉 ALL DATA MATCHES GROUND TRUTH WITH ZERO LOSS!{RESET}")
        print(f"Produced partitions in /var/tmp/mr_worker/*/reduce_output/:")
        for f in cluster_files:
            size = os.path.getsize(f)
            print(f"   • {os.path.basename(f)} ({size} bytes)")

        # Decode binary records to display human-readable word counts
        def decode_records(filepath):
            records = []
            if not os.path.exists(filepath): return records
            with open(filepath, 'rb') as f: data = f.read()
            offset = 0
            while offset < len(data):
                if offset + 4 > len(data): break
                klen = struct.unpack('<I', data[offset:offset+4])[0]; offset += 4
                if offset + klen > len(data): break
                k = data[offset:offset+klen].decode('utf-8', errors='replace'); offset += klen
                if offset + 4 > len(data): break
                vlen = struct.unpack('<I', data[offset:offset+4])[0]; offset += 4
                if offset + vlen > len(data): break
                v = data[offset:offset+vlen].decode('utf-8', errors='replace'); offset += vlen
                records.append((k, v))
            return records

        print(f"\n{BOLD}Sample Word Counts Produced by the Cluster:{RESET}")
        all_records = []
        for f in cluster_files:
            all_records.extend(decode_records(f))
        for k, v in sorted(all_records)[:6]:
            print(f"   {CYAN}{k:12s}{RESET} : {GREEN}{v}{RESET}")
    else:
        print(f"{RED}{BOLD}❌ Output Mismatch!{RESET}")
        print(diff_res.stdout)
        sys.exit(1)

if __name__ == "__main__":
    main()
