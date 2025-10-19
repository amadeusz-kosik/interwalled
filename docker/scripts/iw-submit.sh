#!/usr/bin/env bash

IW_DRIVER_MEMORY="$1"
IW_EXECUTOR_CORES="$2"
IW_TOTAL_EXECUTOR_CORES="$3"
IW_EXECUTOR_MEMORY="$4"
IW_APP_COMMAND="$5"
IW_DATA="$6"
IW_BENCHMARK="$7"

/opt/spark/bin/spark-submit                   \
  --master spark://spark-master:7077                  \
  --deploy-mode client                                \
  --driver-memory   "$IW_DRIVER_MEMORY"               \
  --executor-memory "$IW_EXECUTOR_MEMORY"             \
  --executor-cores  "$IW_EXECUTOR_CORES"              \
  --total-executor-cores "$IW_TOTAL_EXECUTOR_CORES"   \
  --class me.kosik.interwalled.benchmark.Main         \
  /mnt/jar/interwalled-benchmark.jar                  \
  "$IW_APP_COMMAND" "$IW_DATA" "$IW_BENCHMARK"
