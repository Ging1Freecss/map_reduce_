#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

INPUT_FILE="${1:-data/sample.txt}"
INTERMEDIATE_DIR="${2:-/var/tmp/mr_worker}"

python3 "$SCRIPT_DIR/inspect_mr.py" "$INPUT_FILE" "$INTERMEDIATE_DIR"

