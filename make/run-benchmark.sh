#!/usr/bin/env bash

# This file is meant to be run
#  from Makefile by `make` tool.

PROJECT_NAME="$1"
MEMORY="$2"
CORES="$3"
TOTAL_CORES="$4"

export JAR_FILE_PATH="/mnt/jar/benchmark-$PROJECT_NAME/benchmark-$PROJECT_NAME.jar"
export CLASS_NAME="me.kosik.interwalled.benchmark.$PROJECT_NAME.Main"
export IW_DRIVER_MEMORY="$MEMORY"
export IW_EXECUTOR_CORES="$CORES"
export IW_TOTAL_EXECUTOR_CORES="$TOTAL_CORES"
export IW_EXECUTOR_MEMORY="$MEMORY"

docker compose -f docker/docker-compose-$PROJECT_NAME-$MEMORY.yaml up spark-driver