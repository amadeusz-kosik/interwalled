#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_DIR=$( dirname -- "$SCRIPT_DIR" )

# Build & local configuration
export JAVA="/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home/bin/java"
export JAVA_HOME="/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"

JAR="interwalled-benchmark.jar"
JAR_PATH="/mnt/jar/$JAR"

# Local run configuration
LOCAL_JAVA_OPTS=""
LOCAL_JAVA_OPTS="$LOCAL_JAVA_OPTS --add-exports java.base/sun.nio.ch=ALL-UNNAMED"
LOCAL_JAVA_OPTS="$LOCAL_JAVA_OPTS --add-exports java.base/sun.security.action=ALL-UNNAMED"
LOCAL_JAVA_OPTS="$LOCAL_JAVA_OPTS --add-opens java.base/java.lang=ALL-UNNAMED"
LOCAL_JAVA_OPTS="$LOCAL_JAVA_OPTS -Dfile.encoding=UTF-8"
LOCAL_JAVA_OPTS="$LOCAL_JAVA_OPTS "

LOCAL_SPARK_SUBMIT_OPTS=""
LOCAL_SPARK_SUBMIT_OPTS="$LOCAL_SPARK_SUBMIT_OPTS --master spark://localhost:7077"
LOCAL_SPARK_SUBMIT_OPTS="$LOCAL_SPARK_SUBMIT_OPTS --deploy-mode cluster"
LOCAL_SPARK_SUBMIT_OPTS="$LOCAL_SPARK_SUBMIT_OPTS --driver-memory 4G"
LOCAL_SPARK_SUBMIT_OPTS="$LOCAL_SPARK_SUBMIT_OPTS --executor-memory 4G"
LOCAL_SPARK_SUBMIT_OPTS="$LOCAL_SPARK_SUBMIT_OPTS --executor-cores 4"


# Available benchmarks and data suites.
BENCHMARKS=(
#  # Already benchmarked
#  "driver-ailist"
#  "bucketized-scale-2-rdd-ailist"
#  "bucketized-scale-4-rdd-ailist"
#  "bucketized-scale-8-rdd-ailist"
#  "bucketized-scale-16-rdd-ailist"
  "bucketized-scale-32-rdd-ailist"
# Waiting
#  "cached-native-ailist-10-20-10"
#  "cached-native-ailist-10-80-40"
#  "cached-native-ailist-10-320-160"
#  "bucketized-count-100-rdd-ailist"
#  "bucketized-count-1000-rdd-ailist"
#  "bucketized-count-10000-rdd-ailist"
#  "bucketized-count-100000-rdd-ailist"
#  "bucketized-count-1000000-rdd-ailist"
#  "rdd-ailist"
#  "bucketized-count-10-spark-native"
#  "bucketized-count-100-spark-native"
#  "bucketized-count-1000-spark-native"
#  "bucketized-count-10000-spark-native"
#  "bucketized-count-100000-spark-native"
#  "spark-native"
)

DATA_SUITES=(
#  "databio-s-1-2"    # Expected output count:        54 343
#  "databio-s-2-7"    # Expected output count:       274 266
#  "databio-s-1-0"    # Expected output count:       321 138
#  "databio-m-7-0"    # Expected output count:     2 764 185
#  "databio-m-7-3"    # Expected output count:     4 410 928
#  "databio-l-0-8"    # Expected output count:   164 214 743
#  "databio-l-4-8"    # Expected output count:   227 869 400
#  "databio-l-7-8"    # Expected output count:   307 298 107
#  "databio-xl-3-0"   # Expected output count: 1 087 646 273
  "one-to-none-1000"
  "one-to-none-5000"
  "one-to-none-10000"
  "one-to-none-50000"
  "one-to-none-100000"
  "one-to-none-500000"
  "one-to-none-1000000"
  "one-to-none-5000000"
  "one-to-none-10000000"
  "one-to-none-50000000"
  "one-to-none-100000000"
  "one-to-one-1000"
  "one-to-one-5000"
  "one-to-one-10000"
  "one-to-one-50000"
  "one-to-one-100000"
  "one-to-one-500000"
  "one-to-one-1000000"
  "one-to-one-5000000"
  "one-to-one-10000000"
  "one-to-one-50000000"
  "one-to-one-100000000"
)

function build() {
  echo "Building JAR archive."
  sbt clean compile benchmark/assembly
}

# Local: runs
# ---------------------------------------------------------------------------------------------------------------------

function local_run_generator()  {
  java_command="$JAVA $LOCAL_JAVA_OPTS -jar $JAR_PATH test-data-generator"

  echo "Running test data generator."
  echo "$java_command"

  $java_command
}

function local_run_benchmark() {
  data_suite="$1"
  benchmark="$2"

#  java_command="$JAVA $LOCAL_JAVA_OPTS -jar $JAR_PATH benchmark $data_suite $benchmark"
#  java_command="$SPARK_HOME/bin/spark-submit $LOCAL_SPARK_SUBMIT_OPTS --class me.kosik.interwalled.benchmark.Main $JAR_PATH benchmark $data_suite $benchmark"
#  echo "Running for benchmark $benchmark for $data_suite:"
#  echo "$java_command"
#
#  export INTERWALLED_TIMEOUT_AFTER="${ARG_TIMEOUT}"
#
  set +x
  if [[ "$ARG_CARRY_ON" -eq 1 ]]; then set +e; fi
  export IW_DATA="$1"
  export IW_BENCHMARK="$2"
  cd "./docker" && docker compose up spark-driver && cd ../

#  rm -fr temporary/checkpoint/*
#
#  $java_command
  set -e
}

function local_run_benchmarks() {
  for data_suite in "${DATA_SUITES[@]}"; do
    for benchmark in "${BENCHMARKS[@]}"; do
      local_run_benchmark "$data_suite" "$benchmark"
    done
  done
}

## SSH: uploads
#
#function ssh_upload_generator() {
#  echo "Uploading test data generator JAR to the cluster node ($SPARK_SSH_HOST)."
#  scp -i "$SPARK_SSH_KEY"               \
#    "$GENERATOR_JAR_PATH"               \
#    "$SPARK_SSH_USER@$SPARK_SSH_HOST:workdir/$GENERATOR_JAR"
#}
#
#function ssh_upload_benchmark() {
#  echo "Uploading benchmark JAR to the cluster node ($SPARK_SSH_HOST)."
#  scp -i "$SPARK_SSH_KEY"               \
#    "$BENCHMARK_JAR_PATH"               \
#    "$SPARK_SSH_USER@$SPARK_SSH_HOST:workdir/$BENCHMARK_JAR"
#}
#
## SSH: runs
#
#function ssh_run_generator() {
#  echo "Running test data generator."
#
#  ssh -i "$SPARK_SSH_KEY"               \
#    "$SPARK_SSH_USER@$SPARK_SSH_HOST"   \
#    "bash -l -c 'spark-submit ~/workdir/$GENERATOR_JAR spark://$SPARK_SSH_HOST:7077 4G TestDataGenerator true false'"
#}
#
#function ssh_run_benchmark() {
#  data_suite="$1"
#  benchmark="$4"
#
#  for spark_worker_host in "${SPARK_WORKER_HOSTS[@]}"; do
#      ssh -i "$SPARK_SSH_KEY"               \
#        "$SPARK_SSH_USER@$spark_worker_host"   \
#        "bash -l -c 'rm -fr /home/spark/bundle/work/*'"
#  done
#
#  echo "Running for benchmark $benchmark for $data_suite."
#  ssh -i "$SPARK_SSH_KEY"               \
#    "$SPARK_SSH_USER@$SPARK_SSH_HOST"   \
#    "bash -l -c 'INTERWALLED_TIMEOUT_AFTER=\"${ARG_TIMEOUT}\" spark-submit " \
#      "--master spark://$SPARK_SSH_HOST:7077 "  \
#      "--conf spark.standalone.submit.waitAppCompletion=true " \
#      "~/workdir/$BENCHMARK_JAR $data_suite $cluster_size $benchmark'"
#}
#
#function ssh_run_benchmarks() {
#  for cluster_size in "${CLUSTER_SIZES[@]}"; do
#    for data_suite in "${DATA_SUITES[@]}"; do
#      for benchmark in "${BENCHMARKS[@]}"; do
#        ssh_run_benchmark "$data_suite" "$cluster_size" "$benchmark"
#      done
#    done
#  done
#}
#
#function ssh_pull_logs() {
#  BENCHMARK_NAME="$1"
#  BENCHMARK_RESULTS_DIR="$REPO_DIR/jupyter-lab/data"
#
#  BENCHMARK_RESULTS_SOURCE_PATH="jupyter-lab/data"
#  BENCHMARK_RESULTS_TARGET_FILE="$BENCHMARK_RESULTS_DIR/benchmark-$BENCHMARK_NAME.csv"
#
#  echo "Target CSV file: $BENCHMARK_RESULTS_TARGET_FILE."
#
#  echo "Printing CSV header."
#  ssh -i "$SPARK_SSH_KEY"               \
#      "$SPARK_SSH_USER@$SPARK_SSH_HOST"   \
#      "ls \"$BENCHMARK_RESULTS_SOURCE_PATH\" | head -n 1 | xargs -I{} head -n 1 \"$BENCHMARK_RESULTS_SOURCE_PATH/{}\"" \
#      > "$BENCHMARK_RESULTS_TARGET_FILE"
#
#  echo "Printing CSV body."
#  ssh -i "$SPARK_SSH_KEY"               \
#      "$SPARK_SSH_USER@$SPARK_SSH_HOST"   \
#      "ls \"$BENCHMARK_RESULTS_SOURCE_PATH\" | xargs -I{} tail -n +2 \"$BENCHMARK_RESULTS_SOURCE_PATH/{}\"" \
#      >> "$BENCHMARK_RESULTS_TARGET_FILE"
#
#}

# ----------------------------------------------------------------------------------------------------------------------

# CLI handling
ARG_ENV=""
ARG_BUILD=0
ARG_GENERATE=0
ARG_RUN=0
ARG_PULL_LOGS=""
ARG_TIMEOUT=""
ARG_CSV_PATH=""
ARG_CARRY_ON=0

while getopts "he:bgrl:t:c:f" optchar; do
  case "${optchar}" in

    e) ARG_ENV="${OPTARG}" ;;
    b) ARG_BUILD=1 ;;
    g) ARG_GENERATE=1 ;;
    r) ARG_RUN=1 ;;
    l) ARG_PULL_LOGS="${OPTARG}"  ;;
    t) ARG_TIMEOUT="${OPTARG}"    ;;
#    c) ARG_CSV_PATH="${OPTARG}"   ;;
    f) ARG_CARRY_ON=1             ;;

    *)
      echo "Incorrect parameters." >&2
      exit 2
      ;;
  esac
done

if [[ "$ARG_ENV" = "local" ]]; then
  export INTERWALLED_SPARK_MASTER='local[*]'
  export INTERWALLED_SPARK_DRIVER_MEMORY="4G"
  export INTERWALLED_SPARK_EXECUTOR_MEMORY="4G"
  export INTERWALLED_SPARK_EXECUTOR_INSTANCES="4"
  export INTERWALLED_SPARK_EXECUTOR_CORES="1"
  export INTERWALLED_DATA_DIRECTORY="data"

  if [[ "$ARG_BUILD" -eq 1 ]];      then build;                 fi
  if [[ "$ARG_GENERATE" -eq 1 ]];   then local_run_generator;   fi
  if [[ "$ARG_RUN" -eq 1 ]];        then local_run_benchmarks;  fi

elif [[ "$ARG_ENV" = "ssh" ]]; then

  if [[ "$ARG_BUILD" -eq 1 ]];      then
    build
    ssh_upload
  fi

  if [[ "$ARG_GENERATE" -eq 1 ]];   then ssh_run_generator;   fi
  if [[ "$ARG_RUN" -eq 1 ]];        then ssh_run_benchmarks;  fi
  if [[ "$ARG_PULL_LOGS" != "" ]];  then ssh_pull_logs "$ARG_PULL_LOGS"; fi

else
  echo "Unknown or missing environment: $ARG_ENV."
  exit 2;
fi
