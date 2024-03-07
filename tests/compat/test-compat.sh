#!/bin/bash

set -o errexit

usage() {
    echo " Tests the compatibility between different versions of GreptimeDB."
    echo " Expects the directory './bins/current' contains the newly built binaries."
    echo " Usage: $0 <old_version>"
}

# The previous version of GreptimeDB to test compatibility with.
# e.g. old_ver="0.6.0"
old_ver="$1"

if [ -z $old_ver ]
then
    usage
    exit -1
fi

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
echo " === SCRIPT_PATH: $SCRIPT_PATH"
source "${SCRIPT_PATH}/util.sh"

# go to work tree root
cd "$SCRIPT_PATH/../../"

download_binary "$old_ver"

run_test $old_ver "backward"

echo " === Clear GreptimeDB data before running forward compatibility test"
rm -rf /tmp/greptimedb-standalone

run_test $old_ver "forward"

echo "Compatibility test run successfully!"
