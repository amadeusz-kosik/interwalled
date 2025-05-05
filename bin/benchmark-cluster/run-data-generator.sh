#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_BIN_DIR=$( dirname -- "$SCRIPT_DIR" )
REPO_DIR=$( dirname -- "$REPO_BIN_DIR" )

SPARK_SSH_HOST="hp-elitedesk-01"
SPARK_SSH_USER="spark"
SPARK_SSH_KEY="$REPO_DIR/ansible/roles/spark/templates/hp-spark.ssh.private"
GENERATOR_JAR="interwalled-test-data-generator.jar"
GENERATOR_JAR_PATH="$REPO_DIR/test-data-generator/target/scala-2.12/$GENERATOR_JAR"

GENERATE_RESULTS="false"
RUN_EXTENDED_BENCHMARKS="true"

function build() {
  echo "Building JAR archive."
  sbt clean compile testDataGenerator/assembly
}

function upload() {
  echo "Uploading test data generator JAR to the cluster node ($SPARK_SSH_HOST)."
  scp -i "$SPARK_SSH_KEY"               \
    "$GENERATOR_JAR_PATH"               \
    "$SPARK_SSH_USER@$SPARK_SSH_HOST:workdir/$GENERATOR_JAR"
}

function run() {
  echo "Running test data generator."

  ssh -i "$SPARK_SSH_KEY"               \
    "$SPARK_SSH_USER@$SPARK_SSH_HOST"   \
    "bash -l -c 'spark-submit ~/workdir/$GENERATOR_JAR spark://$SPARK_SSH_HOST:7077 2G TestDataGenerator $GENERATE_RESULTS $RUN_EXTENDED_BENCHMARKS'"
}

build
upload
run