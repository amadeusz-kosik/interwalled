#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_BIN_DIR=$( dirname -- "$SCRIPT_DIR" )
REPO_DIR=$( dirname -- "$REPO_BIN_DIR" )

export JAVA="/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home/bin/java"
export JAVA_HOME="/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"

GENERATOR_JAR="./test-data-generator/target/scala-2.12/interwalled-test-data-generator.jar"
BENCHMARK_JAR="./benchmark/target/scala-2.12/interwalled-benchmark.jar"

JAVA_OPTS=""
JAVA_OPTS="$JAVA_OPTS --add-exports java.base/sun.nio.ch=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-exports java.base/sun.security.action=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-opens java.base/java.lang=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS -Dfile.encoding=UTF-8"
JAVA_OPTS="$JAVA_OPTS "

RUN_EXTENDED_BENCHMARKS="false"


function run_generator() {
  java_command="$JAVA $JAVA_OPTS -jar $GENERATOR_JAR $JAVA_ARGUMENTS $RUN_EXTENDED_BENCHMARKS"

  echo "Running test data generator"
  echo "$java_command"

  $java_command
}

function run_benchmark() {
  data_suite="$1"
  benchmark="$2"

  log_files="logs/$data_suite-$benchmark"
  log_files="${log_files// /_}"

  java_command="$JAVA $JAVA_OPTS -jar $BENCHMARK_JAR $JAVA_ARGUMENTS $RUN_EXTENDED_BENCHMARKS $data_suite $benchmark"

  echo "Running for benchmark"
  echo "$java_command"

  $java_command 2>"$log_files.stderr.log" 1> "$log_files.stdout.log"
}

cd "$REPO_DIR" || exit
sbt clean compile

#sbt testDataGenerator/assembly
#run_generator

BENCHMARKS=(
  "broadcast-ailist"
  "partitioned-ailist 100"
  "partitioned-ailist 1000"
  "partitioned-ailist 10000"
  "native-ailist"
  "partitioned-native-ailist-benchmark 1000"
  "spark-native-bucketing 10"
  "spark-native-bucketing 100"
  "spark-native-bucketing 1000"
  "spark-native-bucketing 10000"
)

DATA_SUITES=(
  "one-to-one"
  "one-to-all"
  "all-to-all"
  "spanning-4"
  "spanning-16"
  "continuous-16"
  "sparse-16"
)

sbt benchmark/assembly

set +e

for data_suite in "${DATA_SUITES[@]}"; do
  for benchmark in "${BENCHMARKS[@]}"; do
    run_benchmark "$data_suite" "$benchmark"
  done
done
