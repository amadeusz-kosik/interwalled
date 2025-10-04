#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_DIR=$( dirname -- "$SCRIPT_DIR" )

# Build & local configuration
export JAVA="/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home/bin/java"
export JAVA_HOME="/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"


# Available benchmarks and data suites.
BENCHMARKS=(
#    "cached-native-ailist-10-20-10-64"
#    "checkpointed-native-ailist-10-20-10-64"
#    "driver-ailist"
#    "rdd-ailist-10-20-10-64"
#    "spark-native"
# ----------------------------------------------------------------------------------------------------------------------
  "bucket-per-1000-rdd-ailist-10-20-10-64"
  "bucket-per-1000000-rdd-ailist-10-20-10-64"
  "salt-per-1000000-rdd-ailist-10-20-10-64"
  "salt-per-10000000-rdd-ailist-10-20-10-64"
)

PREPROCESSORS=(
  "bucket-per-1000-"
  "bucket-per-1000000-"
  "bucket-per-1000000000-"
  "salt-per-1000000-"
  "salt-per-10000000-"
  "salt-per-100000000-"
  "salt-per-1000000000-"
)

DATA_SUITES=(
  # odd to even
  "odd-to-even-100000"
  "odd-to-even-500000"
  "odd-to-even-1000000"
  "odd-to-even-5000000"
  "odd-to-even-10000000"
  "odd-to-even-50000000"
  # one to even
  "one-to-even-100000"
  "one-to-even-500000"
  "one-to-even-1000000"
  "one-to-even-5000000"
  "one-to-even-10000000"
  "one-to-even-50000000"
  # one to long cont
  "one-to-long-continuous-100000"
  "one-to-long-continuous-500000"
  "one-to-long-continuous-1000000"
  "one-to-long-continuous-5000000"
  "one-to-long-continuous-10000000"
  "one-to-long-continuous-50000000"
  # one to long overlap
  "one-to-long-overlap-100000"
  "one-to-long-overlap-500000"
  "one-to-long-overlap-1000000"
  "one-to-long-overlap-5000000"
  "one-to-long-overlap-10000000"
  "one-to-long-overlap-50000000"
  # one to one
  "one-to-one-100000"
  "one-to-one-500000"
  "one-to-one-1000000"
  "one-to-one-5000000"
  "one-to-one-10000000"
  "one-to-one-50000000"
  # short cont to long overlap
  "short-continuous-to-long-overlap-100000"
  "short-continuous-to-long-overlap-500000"
  "short-continuous-to-long-overlap-1000000"
  "short-continuous-to-long-overlap-5000000"
  "short-continuous-to-long-overlap-10000000"
  "short-continuous-to-long-overlap-50000000"
  # short cont to random normal
  "short-continuous-to-random-normal-short-100000"
  "short-continuous-to-random-normal-short-500000"
  "short-continuous-to-random-normal-short-1000000"
  "short-continuous-to-random-normal-short-5000000"
  "short-continuous-to-random-normal-short-10000000"
  "short-continuous-to-random-normal-short-50000000"
  # short cont to random poisson
  "short-continuous-to-random-poisson-short-100000"
  "short-continuous-to-random-poisson-short-500000"
  "short-continuous-to-random-poisson-short-1000000"
  "short-continuous-to-random-poisson-short-5000000"
  "short-continuous-to-random-poisson-short-10000000"
  "short-continuous-to-random-poisson-short-10000000"
  "short-continuous-to-random-poisson-short-50000000"
  # short cont to random uniform
  "short-continuous-to-random-uniform-short-100000"
  "short-continuous-to-random-uniform-short-500000"
  "short-continuous-to-random-uniform-short-1000000"
  "short-continuous-to-random-uniform-short-5000000"
  "short-continuous-to-random-uniform-short-10000000"
  "short-continuous-to-random-uniform-short-50000000"
  # short cont to short overlap
  "short-continuous-to-short-overlap-100000"
  "short-continuous-to-short-overlap-500000"
  "short-continuous-to-short-overlap-1000000"
  "short-continuous-to-short-overlap-5000000"
  "short-continuous-to-short-overlap-10000000"
  "short-continuous-to-short-overlap-50000000"
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

function local_run_benchmark() {
  export IW_APP_COMMAND="interval-join-benchmark"
  export IW_DATA="$1"
  export IW_BENCHMARK="$2"

  cd "./docker/$ARG_CLUSTER" && docker compose up spark-driver && cd ../../
}

function local_run_benchmarks() {
  if [[ $"ARG_CLUSTER" == "" ]]; then
    echo "Missing parameter: cluster." >&2
    exit 127
  fi

  for data_suite in "${DATA_SUITES[@]}"; do
    for benchmark in "${BENCHMARKS[@]}"; do
      local_run_benchmark "$data_suite" "$benchmark"
    done
  done
}

function local_run_preprocessor() {
  export IW_APP_COMMAND="partitioning-benchmark"
  export IW_DATA="$1"
  export IW_BENCHMARK="$2"

  cd "./docker/$ARG_CLUSTER" && docker compose up spark-driver && cd ../../
}

function local_run_preprocessors() {
  if [[ $"ARG_CLUSTER" == "" ]]; then
    echo "Missing parameter: cluster." >&2
    exit 127
  fi

  for data_suite in "${DATA_SUITES[@]}"; do
    for preprocessor in "${PREPROCESSORS[@]}"; do
      local_run_preprocessor "$data_suite" "$preprocessor"
    done
  done
}

# ----------------------------------------------------------------------------------------------------------------------

# CLI handling
ARG_CLUSTER=""
ARG_BUILD=0
ARG_GENERATE=0
ARG_RUN=0
ARG_PREPROCESSOR=0

while getopts "c:bgrp" optchar; do
  case "${optchar}" in

    c) ARG_CLUSTER="${OPTARG}"    ;;
    b) ARG_BUILD=1                ;;
    g) ARG_GENERATE=1             ;;
    r) ARG_RUN=1                  ;;
    p) ARG_PREPROCESSOR=1         ;;

    *)
      echo "Incorrect parameters." >&2
      exit 127
      ;;
  esac
done

if [[ "$ARG_BUILD"          -eq 1 ]];   then build;                   fi
if [[ "$ARG_GENERATE"       -eq 1 ]];   then local_run_generator;     fi
if [[ "$ARG_RUN"            -eq 1 ]];   then local_run_benchmarks;    fi
if [[ "$ARG_PREPROCESSOR"   -eq 1 ]];   then local_run_preprocessors; fi

echo "Run wrapper done."
