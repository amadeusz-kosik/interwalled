#!/usr/bin/env bash

/opt/bitnami/spark/bin/spark-submit           \
  --master spark://spark-master:7077          \
  --deploy-mode client                        \
  --driver-memory   "$1"                      \
  --executor-memory "$2"                      \
  --executor-cores  "$3"                      \
  --class me.kosik.interwalled.benchmark.Main \
  /mnt/jar/interwalled-benchmark.jar          \
  "$IW_APP_COMMAND" "$IW_DATA" "$IW_BENCHMARK"
