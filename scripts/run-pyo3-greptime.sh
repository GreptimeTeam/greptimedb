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
            otool -L $GREPTIME_EXEC_PATH | grep -o 'Python.framework/Versions/3.[0-9]\+/Python' | grep -o '3.[0-9]\+'
            ;;
        Linux)
            ldd $GREPTIME_EXEC_PATH | grep -o 'libpython3\.[0-9]\+' | grep -o '3\.[0-9]\+'
            ;;
        *)
            echo "Unsupported OS type: $OS_TYPE"
            exit 1
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

get_optional_args(){
    # if not set by --path path-of-greptime-executable
    # default to search local folder for greptime executable
    ARGS=$(getopt -o "p:" -l "path:" -- "$@")

    # assign ARGS to positional parameters $1, $2, etc...
    eval set -- "$ARGS"
    unset ARGS
    # default path to executable
    exec_path="./greptime"
    # parse for path
    while true; do
        case "$1" in
            -p)
                shift
                exec_path="$1"
                shift
                ;;
            --)
                shift
                break
                ;;
            *)
                break
                ;;
        esac
    done
    export GREPTIME_EXEC_PATH=$exec_path
    export REST_OF_ARGS=$@
}

# Set library path and pass all arguments to greptime to run it
execute_greptime() {
    if [[ "$OS_TYPE" == "Darwin" ]]; then
        DYLD_LIBRARY_PATH="${CONDA_PREFIX:-$PREFIX}/lib:${LD_LIBRARY_PATH:-}" $GREPTIME_EXEC_PATH $@
    elif [[ "$OS_TYPE" == "Linux" ]]; then
        LD_LIBRARY_PATH="${CONDA_PREFIX:-$PREFIX}/lib:${LD_LIBRARY_PATH:-}" $GREPTIME_EXEC_PATH $@
    fi
}

main() {
    get_optional_args $@
    echo GREPTIME_EXEC_PATH: $GREPTIME_EXEC_PATH
    echo REST_OF_ARGS: $REST_OF_ARGS
    local req_py_version
    req_py_version=$(get_python_version)
    readonly req_py_version

    if [[ -z "$req_py_version" ]]; then
        if $GREPTIME_EXEC_PATH --version &> /dev/null; then
            $GREPTIME_EXEC_PATH $REST_OF_ARGS
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
    
    echo "choose your perfered way to install python"
    echo "1. virtualenv"
    echo "2. conda"
    echo "Input your choice [1/2]: "
    read -r option
    case $option in 
        1) 
        setup_virtualenv "$req_py_version"
        ;;
        2) 
        setup_conda_env "$req_py_version"
        ;;
        *) echo "Please input 1 or 2"; exit 1;;
    esac

    execute_greptime $REST_OF_ARGS
}

main "$@"
