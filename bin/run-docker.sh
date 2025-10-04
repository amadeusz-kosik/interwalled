#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_DIR=$( dirname -- "$SCRIPT_DIR" )

cd "$REPO_DIR/docker/$1"

docker compose up spark-master -d
docker compose up spark-worker -d