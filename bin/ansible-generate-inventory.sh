#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_DIR=$( dirname -- "$SCRIPT_DIR" )

PASSWD="$1"

cat <<EOF > "$REPO_DIR/ansible/inventory"
[master]
"hp-elitedesk-01" ansible_sudo_pass='${PASSWD}-HP1'

[workers]
"hp-elitedesk-02" ansible_sudo_pass='${PASSWD}-HP2'
"hp-elitedesk-03" ansible_sudo_pass='${PASSWD}-HP3'
"hp-elitedesk-04" ansible_sudo_pass='${PASSWD}-HP4'

EOF