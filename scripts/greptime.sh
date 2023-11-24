#!/bin/bash

# This script configures the environment to run 'greptime' with the required Python version

# This script should be compatible both in Linux and macOS
OS_TYPE="$(uname)"
readonly OS_TYPE

check_command_existence() {
    command -v "$1" &> /dev/null
}

get_python_version() {
    case "$OS_TYPE" in
        Darwin)
            otool -L greptime | grep -o 'Python.framework/Versions/3.[0-9]+/Python' | grep -o '3.[0-9]+'
            ;;
        Linux)
            ldd greptime | grep -o 'libpython[0-9]\.[0-9]+' | grep -o '[0-9]\.[0-9]+'
            ;;
    esac
}

setup_virtualenv() {
    local req_py_version="$1"
    local env_name="GreptimeTmpVenv$req_py_version"
    virtualenv --python=python"$req_py_version" "$env_name"
    source "$env_name/bin/activate"
}

setup_conda_env() {
    local req_py_version="$1"
    local conda_base
    conda_base=$(conda info --base) || { echo "Error obtaining conda base directory"; exit 1; }
    . "$conda_base/etc/profile.d/conda.sh"

    if ! conda list --name "GreptimeTmpPyO3Env$req_py_version" &> /dev/null; then
        conda create --yes --name "GreptimeTmpPyO3Env$req_py_version" python="$req_py_version"
    fi

    conda activate "GreptimeTmpPyO3Env$req_py_version"
}

# Set library path and pass all arguments to greptime to run it
execute_greptime() {
    if [[ "$OS_TYPE" == "Darwin" ]]; then
        DYLD_LIBRARY_PATH="${CONDA_PREFIX:-$PREFIX}/lib:${LD_LIBRARY_PATH:-}" ./greptime "$@"
    elif [[ "$OS_TYPE" == "Linux" ]]; then
        LD_LIBRARY_PATH="${CONDA_PREFIX:-$PREFIX}/lib:${LD_LIBRARY_PATH:-}" ./greptime "$@"
    fi
}

main() {
    local req_py_version
    req_py_version=$(get_python_version)
    readonly req_py_version

    if [[ -z "$req_py_version" ]]; then
        if ./greptime --version &> /dev/null; then
            ./greptime "$@"
        else
            echo "The 'greptime' binary is not valid or encountered an error."
            exit 1
        fi
        return
    fi

    echo "The required version of Python shared library is $req_py_version"

    echo "Now this script will try to install or find correct Python Version"
    echo "Do you want to continue? (yes/no): "
    read -r yn
    case $yn in
        [Yy]* ) ;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac

    if check_command_existence "virtualenv"; then
        setup_virtualenv "$req_py_version"
    elif check_command_existence "conda"; then
        setup_conda_env "$req_py_version"
    else
        echo "Neither virtualenv nor conda is available."
        exit 1
    fi

    execute_greptime "$@"
}

main "$@"