#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_DIR=$( dirname -- "$SCRIPT_DIR" )

cd "$REPO_DIR"

if [[ ! -d ".venv" ]]; then
  python3 -m venv .venv
  source .venv/bin/activate
  pip3 install -r ./requirements.txt
else
  source .venv/bin/activate
fi

jupyter lab
