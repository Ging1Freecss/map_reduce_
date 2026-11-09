#!/usr/bin/env bash
INPUT_FILE="${1:-data/sample.txt}"
INTERMEDIATE_DIR="${2:-/tmp/mr_worker_intermediate}"

./inspect_mr.py "$INPUT_FILE" "$INTERMEDIATE_DIR"

