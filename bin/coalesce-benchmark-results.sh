#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_DIR=$( dirname -- "$SCRIPT_DIR" )
BENCHMARK_RESULTS_DIR="$REPO_DIR/jupyter-lab/data"

BENCHMARK_NAME="$1"
BENCHMARK_RESULTS_SOURCE_PATH="$BENCHMARK_RESULTS_DIR/benchmark-$BENCHMARK_NAME"
BENCHMARK_RESULTS_TARGET_FILE="$BENCHMARK_RESULTS_DIR/benchmark-$BENCHMARK_NAME.csv"

echo "Printing CSV header."
ls "$BENCHMARK_RESULTS_SOURCE_PATH" | head -n 1 | xargs -I{} head -n 1 "$BENCHMARK_RESULTS_SOURCE_PATH/{}" \
  > "$BENCHMARK_RESULTS_TARGET_FILE"

echo "Printing CSV body."
ls "$BENCHMARK_RESULTS_SOURCE_PATH" | xargs -I{} tail -n +2 "$BENCHMARK_RESULTS_SOURCE_PATH/{}" \
  >> "$BENCHMARK_RESULTS_TARGET_FILE"
