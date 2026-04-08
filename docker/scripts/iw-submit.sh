#!/usr/bin/env bash

JAR_NAME="$1"
CLASS_NAME="$2"

/opt/spark/bin/spark-submit                           \
  --master spark://spark-master:7077                  \
  --deploy-mode client                                \
  --driver-memory   "$IW_DRIVER_MEMORY"               \
  --executor-memory "$IW_EXECUTOR_MEMORY"             \
  --executor-cores  "$IW_EXECUTOR_CORES"              \
  --conf spark.eventLog.enabled=true                  \
  --conf spark.eventLog.dir=file:/mnt/spark-events    \
  --total-executor-cores "$IW_TOTAL_EXECUTOR_CORES"   \
  --class "$CLASS_NAME"                               \
  "$JAR_NAME"                                         \
  spark://spark-master:7077
