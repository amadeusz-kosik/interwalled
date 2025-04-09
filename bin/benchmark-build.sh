#!/usr/bin/env bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

source "$PROJECT_DIR/.env/cluster.sh"

cd "$PROJECT_DIR" || exit
sbt benchmark/clean benchmark/assembly

scp -i "$IW_CLUSTER_MASTER_KEY"                                           \
    "$PROJECT_DIR/benchmark/target/scala-2.12/interwalled-benchmark.jar"  \
    "$IW_CLUSTER_MASTER_USER@$IW_CLUSTER_MASTER_HOST:interwalled-benchmark.jar"