#!/usr/bin/env bash
set -e

# Configuration with defaults (can be overridden via CLI arguments or env vars)
INPUT_FILE="${1:-data/sample.txt}"
NUM_REDUCERS="${2:-4}"
SPLIT_SIZE="${3:-30}"
NUM_WORKERS="${4:-3}"
WORKER_DELAY="${5:-2}"  # delay in seconds between starting workers
COORD_PORT="50051"
WORKER_BASE_PORT="50052"
LOG_DIR="logs"

mkdir -p "$LOG_DIR"

cleanup() {
    echo ""
    echo "[Launcher] Stopping cluster processes..."
    # Disable job monitor warnings
    set +e
    kill -15 $(jobs -p) 2>/dev/null || true
    sleep 1
    kill -9 $(jobs -p) 2>/dev/null || true
    killall -9 coordinator worker 2>/dev/null || true
    echo "[Launcher] Cleanup complete."
}

trap cleanup EXIT INT TERM

echo "=============================================="
echo " MapReduce Cluster Launcher"
echo "=============================================="
echo " Input File:      $INPUT_FILE"
echo " Reducers:        $NUM_REDUCERS"
echo " Split Size:      $SPLIT_SIZE bytes"
echo " Worker Count:    $NUM_WORKERS"
echo " Worker Delay:    ${WORKER_DELAY}s"
echo " Log Directory:   $LOG_DIR/"
echo "=============================================="

# 1. Clean previous state
echo "[1/4] Cleaning previous run artifacts..."
killall -9 coordinator worker 2>/dev/null || true
rm -rf /tmp/mr_worker_intermediate data/sample.txt.map_data.* "$LOG_DIR"/*

# 2. Build binaries
echo "[2/4] Building latest binaries..."
cmake --build build

# 3. Start Coordinator
echo "[3/4] Starting Coordinator on port $COORD_PORT..."
./build/coordinator/coordinator "$INPUT_FILE" -p "$COORD_PORT" -r "$NUM_REDUCERS" -s "$SPLIT_SIZE" > "$LOG_DIR/coordinator.log" 2>&1 &
COORD_PID=$!
echo "      Coordinator PID: $COORD_PID (logging to $LOG_DIR/coordinator.log)"

# Wait for Coordinator to bind
sleep 1

# 4. Start Workers staggered with delay
echo "[4/4] Starting $NUM_WORKERS Worker(s) with ${WORKER_DELAY}s delay..."
for (( i=0; i<NUM_WORKERS; i++ )); do
    W_PORT=$(( WORKER_BASE_PORT + i ))
    W_LOG="$LOG_DIR/worker_${i}.log"
    
    echo "      --> Launching Worker $i on port $W_PORT..."
    ./build/worker/worker -c "127.0.0.1:$COORD_PORT" -p "$W_PORT" > "$W_LOG" 2>&1 &
    W_PID=$!
    echo "          PID: $W_PID (logging to $W_LOG)"

    if (( i < NUM_WORKERS - 1 )); then
        echo "          Waiting ${WORKER_DELAY}s before next worker..."
        sleep "$WORKER_DELAY"
    fi
done

echo ""
echo "=============================================="
echo " Cluster is running! Press Ctrl+C to terminate."
echo "=============================================="
echo ""

# Follow the coordinator log in the terminal
tail -f "$LOG_DIR/coordinator.log" &
TAIL_PID=$!

# Wait for coordinator process to finish
wait $COORD_PID || true
kill $TAIL_PID 2>/dev/null || true
