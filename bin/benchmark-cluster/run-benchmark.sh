#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_BIN_DIR=$( dirname -- "$SCRIPT_DIR" )
REPO_DIR=$( dirname -- "$REPO_BIN_DIR" )

export JAVA="/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home/bin/java"
export JAVA_HOME="/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"

SPARK_SSH_HOST="hp-elitedesk-01"
SPARK_SSH_USER="spark"
SPARK_SSH_KEY="$REPO_DIR/ansible/roles/spark/templates/hp-spark.ssh.private"
BENCHMARK_JAR="interwalled-benchmark.jar"
BENCHMARK_JAR_PATH="$REPO_DIR/benchmark/target/scala-2.12/$BENCHMARK_JAR"

RUN_EXTENDED_BENCHMARKS="true"

function build() {
  echo "Building JAR archive."
  sbt clean compile benchmark/assembly
}

function upload() {
  echo "Uploading benchmark JAR to the cluster node ($SPARK_SSH_HOST)."
  scp -i "$SPARK_SSH_KEY"               \
    "$BENCHMARK_JAR_PATH"               \
    "$SPARK_SSH_USER@$SPARK_SSH_HOST:workdir/$BENCHMARK_JAR"
}

function run() {
  echo "Running benchmark."

  BENCHMARKS=(
    "broadcast-ailist"
#    "partitioned-native-ailist-benchmark 10"
#    "partitioned-native-ailist-benchmark 100"
#    "partitioned-native-ailist-benchmark 1000"
#    "partitioned-native-ailist-benchmark 10000"
  )

  DATA_SUITES=(
#    "one-to-one"
    "one-to-all"
    "all-to-all"
    "spanning-4"
    "spanning-16"
    "continuous-16"
    "sparse-16"
  )

  set +e

  for data_suite in "${DATA_SUITES[@]}"; do
    for benchmark in "${BENCHMARKS[@]}"; do
      ssh -i "$SPARK_SSH_KEY"               \
        "$SPARK_SSH_USER@$SPARK_SSH_HOST"   \
        "bash -l -c 'spark-submit "                 \
          "--master spark://$SPARK_SSH_HOST:7077 "  \
          "--conf spark.standalone.submit.waitAppCompletion=true " \
          "~/workdir/$BENCHMARK_JAR  $RUN_EXTENDED_BENCHMARKS $data_suite $benchmark'"
    done
  done
}

#build
#upload
run