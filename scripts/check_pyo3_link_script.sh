#!/bin/bash
# This script is only used in CI to check if the pyo3 backend is linked correctly
echo $(pwd)
if [[ $FEATURES == *pyo3_backend* ]]; then
    cp target/release/greptime scripts
    if yes | ./scripts/greptime.sh --version &> /dev/null; then
        exit 0
    else
        exit 1
    fi
fi