#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_DIR=$( dirname -- "$SCRIPT_DIR" )

# Build & local configuration
export JAVA="/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home/bin/java"
export JAVA_HOME="/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"

GENERATOR_JAR="interwalled-test-data-generator.jar"
GENERATOR_JAR_PATH="$REPO_DIR/test-data-generator/target/scala-2.12/$GENERATOR_JAR"
BENCHMARK_JAR="interwalled-benchmark.jar"
BENCHMARK_JAR_PATH="$REPO_DIR/benchmark/target/scala-2.12/$BENCHMARK_JAR"

# Local run configuration
LOCAL_JAVA_OPTS=""
LOCAL_JAVA_OPTS="$LOCAL_JAVA_OPTS --add-exports java.base/sun.nio.ch=ALL-UNNAMED"
LOCAL_JAVA_OPTS="$LOCAL_JAVA_OPTS --add-exports java.base/sun.security.action=ALL-UNNAMED"
LOCAL_JAVA_OPTS="$LOCAL_JAVA_OPTS --add-opens java.base/java.lang=ALL-UNNAMED"
LOCAL_JAVA_OPTS="$LOCAL_JAVA_OPTS -Dfile.encoding=UTF-8"
LOCAL_JAVA_OPTS="$LOCAL_JAVA_OPTS "

# Remote run configuration
SPARK_SSH_HOST="hp-elitedesk-01"
SPARK_SSH_USER="spark"
SPARK_SSH_KEY="$REPO_DIR/ansible/roles/spark/templates/hp-spark.ssh.private"

SPARK_WORKER_HOSTS=(
  "hp-elitedesk-02"
  "hp-elitedesk-03"
  "hp-elitedesk-04"
)

# Available benchmarks and data suites.
BENCHMARKS=(
  "broadcast-ailist"
  "partitioned-native-ailist-benchmark     100 8"
  "partitioned-native-ailist-benchmark    1000 8"
  "partitioned-native-ailist-benchmark   10000 8"
  "partitioned-native-ailist-benchmark  100000 8"
)

CLUSTERS_COUNTS=(
  "1"
)

CLUSTER_SIZES=(
     "10000"  #  10K
     "25000"
     "50000"
     "75000"
#    "100000" # 100K
#    "250000"
#    "500000"
#    "750000"
#   "1000000"
#   "2500000"
#   "5000000"
#  "10000000"
#  "25000000"
#  "50000000"
)

DATA_SUITES=(
  "one-to-one"
  "one-to-all"
  "all-to-one"
  "all-to-all"
  "spanning-4"
  "spanning-16"
  "continuous-16"
  "sparse-16"
)

function build() {
  echo "Building JAR archive."
  sbt clean compile benchmark/assembly testDataGenerator/assembly
}

# Local: runs

function local_run_generator()  {
  java_command="$JAVA $LOCAL_JAVA_OPTS -jar $GENERATOR_JAR_PATH false false"

  echo "Running test data generator."
  echo "$java_command"

  $java_command
}

function local_run_benchmark() {
  data_suite="$1"
  cluster_count="$2"
  cluster_size="$3"
  benchmark="$4"

  java_command=" $JAVA $LOCAL_JAVA_OPTS -jar $BENCHMARK_JAR_PATH $data_suite $cluster_count $cluster_size $benchmark $ARG_CSV_PATH"
  echo "Running for benchmark $benchmark for $data_suite: $java_command"

  export INTERWALLED_TIMEOUT_AFTER="${ARG_TIMEOUT}"
  $java_command
}

function local_run_benchmarks() {
#  set +e

  for cluster_count in "${CLUSTERS_COUNTS[@]}"; do
    for cluster_size in "${CLUSTER_SIZES[@]}"; do
      for data_suite in "${DATA_SUITES[@]}"; do
        for benchmark in "${BENCHMARKS[@]}"; do
          local_run_benchmark "$data_suite" "$cluster_count" "$cluster_size" "$benchmark"
        done
      done
    done
  done
}

# SSH: uploads

function ssh_upload_generator() {
  echo "Uploading test data generator JAR to the cluster node ($SPARK_SSH_HOST)."
  scp -i "$SPARK_SSH_KEY"               \
    "$GENERATOR_JAR_PATH"               \
    "$SPARK_SSH_USER@$SPARK_SSH_HOST:workdir/$GENERATOR_JAR"
}

function ssh_upload_benchmark() {
  echo "Uploading benchmark JAR to the cluster node ($SPARK_SSH_HOST)."
  scp -i "$SPARK_SSH_KEY"               \
    "$BENCHMARK_JAR_PATH"               \
    "$SPARK_SSH_USER@$SPARK_SSH_HOST:workdir/$BENCHMARK_JAR"
}

# SSH: runs

function ssh_run_generator() {
  echo "Running test data generator."

  ssh -i "$SPARK_SSH_KEY"               \
    "$SPARK_SSH_USER@$SPARK_SSH_HOST"   \
    "bash -l -c 'spark-submit ~/workdir/$GENERATOR_JAR spark://$SPARK_SSH_HOST:7077 4G TestDataGenerator true false'"
}

function ssh_run_benchmark() {
  data_suite="$1"
  clusters_count="$2"
  clusters_size="$3"
  benchmark="$4"

  for spark_worker_host in "${SPARK_WORKER_HOSTS[@]}"; do
      ssh -i "$SPARK_SSH_KEY"               \
        "$SPARK_SSH_USER@$spark_worker_host"   \
        "bash -l -c 'rm -fr /home/spark/bundle/work/*'"
  done

  echo "Running for benchmark $benchmark for $data_suite."
  ssh -i "$SPARK_SSH_KEY"               \
    "$SPARK_SSH_USER@$SPARK_SSH_HOST"   \
    "bash -l -c 'INTERWALLED_TIMEOUT_AFTER=\"${ARG_TIMEOUT}\" spark-submit " \
      "--master spark://$SPARK_SSH_HOST:7077 "  \
      "--conf spark.standalone.submit.waitAppCompletion=true " \
      "~/workdir/$BENCHMARK_JAR $data_suite $clusters_count $cluster_size $benchmark'"
}

function ssh_run_benchmarks() {
  for cluster_count in "${CLUSTERS_COUNTS[@]}"; do
    for cluster_size in "${CLUSTER_SIZES[@]}"; do
      for data_suite in "${DATA_SUITES[@]}"; do
        for benchmark in "${BENCHMARKS[@]}"; do
          ssh_run_benchmark "$data_suite" "$cluster_count" "$cluster_size" "$benchmark"
        done
      done
    done
  done
}

function ssh_pull_logs() {
  BENCHMARK_NAME="$1"
  BENCHMARK_RESULTS_DIR="$REPO_DIR/jupyter-lab/data"

  BENCHMARK_RESULTS_SOURCE_PATH="jupyter-lab/data"
  BENCHMARK_RESULTS_TARGET_FILE="$BENCHMARK_RESULTS_DIR/benchmark-$BENCHMARK_NAME.csv"

  echo "Target CSV file: $BENCHMARK_RESULTS_TARGET_FILE."

  echo "Printing CSV header."
  ssh -i "$SPARK_SSH_KEY"               \
      "$SPARK_SSH_USER@$SPARK_SSH_HOST"   \
      "ls \"$BENCHMARK_RESULTS_SOURCE_PATH\" | head -n 1 | xargs -I{} head -n 1 \"$BENCHMARK_RESULTS_SOURCE_PATH/{}\"" \
      > "$BENCHMARK_RESULTS_TARGET_FILE"

  echo "Printing CSV body."
  ssh -i "$SPARK_SSH_KEY"               \
      "$SPARK_SSH_USER@$SPARK_SSH_HOST"   \
      "ls \"$BENCHMARK_RESULTS_SOURCE_PATH\" | xargs -I{} tail -n +2 \"$BENCHMARK_RESULTS_SOURCE_PATH/{}\"" \
      >> "$BENCHMARK_RESULTS_TARGET_FILE"

}

# ----------------------------------------------------------------------------------------------------------------------

# CLI handling
ARG_ENV=""
ARG_BUILD=0
ARG_GENERATE=0
ARG_RUN=0
ARG_PULL_LOGS=""
ARG_TIMEOUT=""
ARG_CSV_PATH=""

while getopts "he:bgrl:t:c:" optchar; do
  case "${optchar}" in

    e) ARG_ENV="${OPTARG}" ;;
    b) ARG_BUILD=1 ;;
    g) ARG_GENERATE=1 ;;
    r) ARG_RUN=1 ;;
    l) ARG_PULL_LOGS="${OPTARG}"  ;;
    t) ARG_TIMEOUT="${OPTARG}"    ;;
    c) ARG_CSV_PATH="${OPTARG}"   ;;

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
    ssh_upload_generator
    ssh_upload_benchmark
  fi

  if [[ "$ARG_GENERATE" -eq 1 ]];   then ssh_run_generator;   fi
  if [[ "$ARG_RUN" -eq 1 ]];        then ssh_run_benchmarks;  fi
  if [[ "$ARG_PULL_LOGS" != "" ]];  then ssh_pull_logs "$ARG_PULL_LOGS"; fi

else
  echo "Unknown or missing environment: $ARG_ENV."
  exit 2;
fi
