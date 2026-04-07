#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_DIR=$( dirname -- "$SCRIPT_DIR" )

# Build & local configuration
export JAVA="/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home/bin/java"
export JAVA_HOME="/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"



DATA_SUITES=(
  # databio
  "databio-s-1-2"    # Expected output count:        54 343
  "databio-s-2-7"    # Expected output count:       274 266
  "databio-s-1-0"    # Expected output count:       321 138
  "databio-m-7-0"    # Expected output count:     2 764 185
  "databio-m-7-3"    # Expected output count:     4 410 928
  "databio-l-0-8"    # Expected output count:   164 214 743
  "databio-l-4-8"    # Expected output count:   227 869 400
  "databio-l-7-8"    # Expected output count:   307 298 107
  "databio-xl-3-0"   # Expected output count: 1 087 646 273
)

function build() {
  echo "Building JAR archive."
  sbt clean compile benchmark/assembly
}

function local_run_generator() {
  export IW_APP_COMMAND="test-data-generator"
  export IW_DATA="unit-test-data"
  export IW_BENCHMARK=""

  export IW_DRIVER_MEMORY="4G"
  export IW_TOTAL_EXECUTOR_CORES="20"
  export IW_EXECUTOR_CORES="10"
  export IW_EXECUTOR_MEMORY="10G"

  cd "./docker/" && docker compose up spark-driver && cd ../
}

function local_run_benchmark() {
  export IW_APP_COMMAND="interval-join-benchmark"
  export IW_DATA="$1"
  export IW_BENCHMARK="$2"

  export IW_DRIVER_MEMORY="4G"
  export IW_TOTAL_EXECUTOR_CORES="20"

  case "$ARG_CLUSTER" in
    s | small)
      export IW_EXECUTOR_CORES="2"
      export IW_EXECUTOR_MEMORY="2G"
      ;;

    m | medium)
      export IW_EXECUTOR_CORES="4"
      export IW_EXECUTOR_MEMORY="4G"
      ;;

    l | large)
      export IW_EXECUTOR_CORES="10"
      export IW_EXECUTOR_MEMORY="10G"
      ;;

    *)
      echo "Incorrect cluster sizing: $ARG_CLUSTER. Use one of: s|m|l." >&2
      exit 127
      ;;

  esac

  cd "./docker/" && docker compose up spark-driver && cd ../
}

function local_run_benchmarks() {
  if [[ "$ARG_CLUSTER" == "" ]]; then
    echo "Missing parameter: cluster." >&2
    exit 127
  fi

  for data_suite in "${DATA_SUITES[@]}"; do
    for benchmark in "${BENCHMARKS[@]}"; do
      local_run_benchmark "$data_suite" "$benchmark"
    done
  done
}

# ----------------------------------------------------------------------------------------------------------------------

# CLI handling
ARG_CLUSTER=""
ARG_BUILD=0
ARG_GENERATE=0
ARG_RUN=0

while getopts "c:bgr" optchar; do
  case "${optchar}" in

    c) ARG_CLUSTER="${OPTARG}"    ;;
    b) ARG_BUILD=1                ;;
    g) ARG_GENERATE=1             ;;
    r) ARG_RUN=1                  ;;

    *)
      echo "Incorrect parameters." >&2
      exit 127
      ;;
  esac
done

if [[ "$ARG_BUILD"          -eq 1 ]];   then build;                   fi
if [[ "$ARG_GENERATE"       -eq 1 ]];   then local_run_generator;     fi
if [[ "$ARG_RUN"            -eq 1 ]];   then local_run_benchmarks;    fi

echo "Run wrapper done."
