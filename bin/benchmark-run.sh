#!/usr/bin/env bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

source "$PROJECT_DIR/.env/cluster.sh"

cd "$PROJECT_DIR" || exit

ssh -i "$IW_CLUSTER_MASTER_KEY"                                           \
    "$IW_CLUSTER_MASTER_USER@$IW_CLUSTER_MASTER_HOST"                     \
    "spark-submit ~/interwalled-benchmark.jar spark://192.168.0.128:7077 2G SparkNaiveBucketingBenchmark hdfs:///interwalled/mirror-join/input-80M.bed hdfs:///interwalled/mirror-join/input-80M.bed 100,200,400,800,1600,3200"