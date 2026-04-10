#!/usr/bin/env bash

# This file is meant to be run
#  from Makefile by `make` tool.

PROJECT_NAME="$1"
MEMORY="$2"
CORES="$3"
TOTAL_CORES="$4"
SPARK_VERSION="$5"

COMPOSE_YAML_FILE="./docker/docker-compose-$PROJECT_NAME-$MEMORY.yaml"

export JAR_FILE_PATH="/mnt/jar/benchmark-$PROJECT_NAME/benchmark-$PROJECT_NAME.jar"
export CLASS_NAME="me.kosik.interwalled.benchmark.$PROJECT_NAME.Main"
export IW_DRIVER_MEMORY="$MEMORY"
export IW_EXECUTOR_CORES="$CORES"
export IW_TOTAL_EXECUTOR_CORES="$TOTAL_CORES"
export IW_EXECUTOR_MEMORY="$MEMORY"


echo "Create docker compose file."
cat ./docker/template/docker-compose.yaml.j2        \
		| sed "s/{{ spark_version }}/$SPARK_VERSION/g"  \
		| sed "s/{{ driver_memory }}/$MEMORY/g"         \
		> $COMPOSE_YAML_FILE

echo "Start Apache Spark cluster."
docker compose -f $COMPOSE_YAML_FILE up spark-master -d
docker compose -f $COMPOSE_YAML_FILE up spark-worker -d
docker compose -f $COMPOSE_YAML_FILE up spark-history-server -d
