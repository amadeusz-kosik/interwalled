# shellcheck shell=sh

java_bin="$(update-alternatives --list java | head -n 1)"
java_bin_directory="$(dirname "$java_bin")"
java_home_directory="$(dirname "$java_bin_directory")"

export JAVA_HOME="$java_home_directory"